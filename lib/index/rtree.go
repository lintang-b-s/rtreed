package index

import (
	"bytes"
	"container/heap"
	"errors"
	"math"
	"sort"
	"sync"

	"github.com/lintang-b-s/lbs/types"
)

type DalI interface {
	GetNode(pageNum types.Pgnum) (*Node, error)
	WriteNode(n *Node) (*Node, error)
	UpdateMetaHeightSize(height int, size int32)
	UpdateMetaRoot(root types.Pgnum)
}

type Rtree struct {
	Dal        DalI
	Dim        int
	MinEntries int
	MaxEntries int
	root       types.Pgnum
	size       int32
	height     int
	latch      sync.RWMutex
}

func NewTree(dim, min, max int, root types.Pgnum, mHeight int, mSize int32) *Rtree {
	var height int
	if mHeight != 0 {
		height = mHeight
	} else {
		height = 1
	}

	rt := &Rtree{
		Dim:        dim,
		MinEntries: min,
		MaxEntries: max,
		height:     height,
		root:       root,
		size:       mSize,
	}

	return rt
}

type Node struct {
	Dal    DalI
	parent types.Pgnum

	PageNum types.Pgnum
	entries []Entry
	level   int
	leaf    bool
}

type Entry struct {
	rect  Rect
	child types.Pgnum
	obj   SpatialData
}

type SpatialData struct {
	Location Point
	Data     []byte
}

var tol = 0.0001

func (s *SpatialData) Bounds() Rect {
	return s.Location.ToRect(tol)
}

func (tree *Rtree) Insert(obj SpatialData) {
	e := Entry{obj.Bounds(), 0, obj}
	tree.latch.Lock()
	tree.insert(e, 1)

	tree.size++
	tree.Dal.UpdateMetaHeightSize(tree.height, tree.size)
	tree.latch.Unlock()
}

func (tree *Rtree) insert(e Entry, level int) {

	root, err := tree.Dal.GetNode(tree.root)
	if err != nil {
		panic(err)
	}
	var leaf *Node
	if level != 1 {
		leaf = tree.chooseNode(root, e, level)
	} else {
		leaf = tree.chooseLeaf(root, e)
	}

	leaf.entries = append(leaf.entries, e)

	eChild, _ := tree.Dal.GetNode(e.child)

	if eChild != nil {
		eChild.parent = leaf.PageNum
		tree.Dal.WriteNode(eChild)
	}

	leaf, err = tree.Dal.WriteNode(leaf)
	if err != nil {
		panic(err)
	}

	var ll *Node
	if len(leaf.entries) > tree.MaxEntries {
		leaf, ll = leaf.splitNode(tree.MinEntries)
	}

	l, ll := tree.adjustTree(leaf, ll)
	if ll != nil && l.PageNum == root.PageNum {
		oldRoot := l
		tree.height++

		ll, err = tree.Dal.WriteNode(ll)
		if err != nil {
			panic(err)
		}
		newRoot := &Node{
			parent: 0,
			level:  tree.height,
			entries: []Entry{
				{rect: createNodeRectangle(*oldRoot), child: oldRoot.PageNum, obj: SpatialData{}},
				{rect: createNodeRectangle(*ll), child: ll.PageNum, obj: SpatialData{}},
			},
		}
		newRoot, err = tree.Dal.WriteNode(newRoot)
		if err != nil {
			panic(err)
		}
		tree.root = newRoot.PageNum
		tree.Dal.UpdateMetaRoot(newRoot.PageNum)

		oldRoot.parent = tree.root
		ll.parent = tree.root
		tree.Dal.WriteNode(ll)
		tree.Dal.WriteNode(oldRoot)
	}
}

func chooseLeastEnlargement(entries []Entry, e Entry) types.Pgnum {
	rectAreaDiff := math.MaxFloat64
	var leastEnlEntry Entry
	for _, en := range entries {
		rect := createRectangle(en.rect, e.rect)
		currDiff := rect.Area() - en.rect.Area()
		if currDiff < rectAreaDiff || (currDiff == rectAreaDiff && en.rect.Area() < leastEnlEntry.rect.Area()) {
			rectAreaDiff = currDiff
			leastEnlEntry = en
		}
	}
	return leastEnlEntry.child
}

// chooseNode finds the node at the specified level to which e should be added.
func (tree *Rtree) chooseNode(n *Node, e Entry, level int) *Node {
	if n.level == level {
		return n
	}
	chosenChild := chooseLeastEnlargement(n.entries, e)

	if chosenChild == 0 {
		return n
	}
	child, err := tree.Dal.GetNode(chosenChild)
	if err != nil {
		panic(err)
	}
	return tree.chooseNode(child, e, level)
}

func (tree *Rtree) chooseLeaf(n *Node, e Entry) *Node {

	if n.leaf {
		return n
	}

	chosenChild := chooseLeastEnlargement(n.entries, e)

	if chosenChild == 0 {
		return n
	}

	child, err := tree.Dal.GetNode(chosenChild)
	if err != nil {
		panic(err)
	}
	return tree.chooseLeaf(child, e)
}

func (tree *Rtree) adjustTree(l, ll *Node) (*Node, *Node) {
	root, err := tree.Dal.GetNode(tree.root)
	if err != nil {
		panic(err)
	}
	if l.PageNum == root.PageNum {
		tree.Dal.WriteNode(l)
		if ll != nil {
			tree.Dal.WriteNode(ll)
		}
		return l, ll
	}

	en, idx := l.getNFromParentEntry()
	prevRect := en.rect
	en.rect = createNodeRectangle(*l)

	nParent, err := tree.Dal.GetNode(l.parent)
	nParent.entries[idx] = *en

	if err != nil {
		panic(err)
	}
	if ll == nil {
		tree.Dal.WriteNode(l)
		tree.Dal.WriteNode(nParent)

		if en.rect.Equal(prevRect) {
			return root, nil
		}
		return tree.adjustTree(nParent, nil)
	}

	ell := Entry{createNodeRectangle(*ll), ll.PageNum, SpatialData{}}
	nParent.entries = append(nParent.entries, ell)

	tree.Dal.WriteNode(nParent)
	tree.Dal.WriteNode(l)
	tree.Dal.WriteNode(ll)

	if len(nParent.entries) < tree.MaxEntries {
		return tree.adjustTree(nParent, nil)
	}

	return tree.adjustTree(nParent.splitNode(tree.MinEntries))
}

func (n *Node) getNFromParentEntry() (*Entry, int) {
	var e *Entry
	idx := -1
	nParent, err := n.Dal.GetNode(n.parent)
	if err != nil {
		panic(err)
	}
	for i := range nParent.entries {
		nEntryChild, _ := n.Dal.GetNode(nParent.entries[i].child)
		if nEntryChild.PageNum == n.PageNum {
			return &nParent.entries[i], i
		}
	}
	return e, idx
}

func createNodeRectangle(node Node) Rect {
	if len(node.entries) == 1 {
		return node.entries[0].rect

	}

	rect := createRectangle(node.entries[0].rect, node.entries[1].rect)
	for i := 2; i < len(node.entries); i++ {
		e := node.entries[i]
		rect = createRectangle(rect, e.rect)
	}
	return rect
}

func (n *Node) pickSeeds() (int, int) {
	var entryOneIDx, entryTwoIDx int
	maxD := math.Inf(-1)
	for i, e1 := range n.entries {
		for j := i + 1; j < len(n.entries); j++ {
			e2 := n.entries[j]
			areaJ := createRectangle(e1.rect, e2.rect).Area()
			d := areaJ - e1.rect.Area() - e2.rect.Area()
			if d > maxD {
				maxD = d
				entryOneIDx = i
				entryTwoIDx = j
			}
		}
	}
	return entryOneIDx, entryTwoIDx
}

func (n *Node) splitNode(minGroupSize int) (*Node, *Node) {
	entryOneIDx, entryTwoIDx := n.pickSeeds()
	entryOne, entryTwo := n.entries[entryOneIDx], n.entries[entryTwoIDx]

	otherEntriesOne := append(n.entries[:entryOneIDx], n.entries[entryOneIDx+1:entryTwoIDx]...)
	otherEntriesTwo := n.entries[entryTwoIDx+1:]
	otherEntries := append(otherEntriesOne, otherEntriesTwo...)

	groupOne := n
	groupOne.entries = []Entry{entryOne}
	groupTwo := &Node{
		parent:  n.parent,
		leaf:    n.leaf,
		level:   n.level,
		entries: []Entry{entryTwo},
	}

	groupTwo, err := n.Dal.WriteNode(groupTwo)

	if err != nil {
		panic(err)
	}

	entryTwoChild, err := n.Dal.GetNode(entryTwo.child)
	if err != nil {
		panic(err)
	}

	entryOneChild, err := n.Dal.GetNode(entryOne.child)
	if err != nil {
		panic(err)
	}

	if entryTwoChild != nil {
		entryTwoChild.parent = groupTwo.PageNum
	}
	if entryOneChild != nil {
		entryOneChild.parent = groupOne.PageNum
	}

	n.Dal.WriteNode(entryTwoChild)
	n.Dal.WriteNode(entryOneChild)

	for len(otherEntries) > 0 {
		next := pickNext(groupOne, groupTwo, otherEntries)
		e := otherEntries[next]

		if len(otherEntries)+len(groupOne.entries) <= minGroupSize {
			n.assignEntryToGroup(e, groupOne)
		} else if len(otherEntries)+len(groupTwo.entries) <= minGroupSize {
			n.assignEntryToGroup(e, groupTwo)
		} else {

			gOneRect := createNodeRectangle(*groupOne)
			gTwoRect := createNodeRectangle(*groupTwo)
			gOneEntryRect := createRectangle(gOneRect, e.rect)
			gTwoEntryRect := createRectangle(gTwoRect, e.rect)

			gOneEnlargement := gOneEntryRect.Area() - gOneRect.Area()
			gTwoEnlargement := gTwoEntryRect.Area() - gTwoRect.Area()
			if gOneEnlargement < gTwoEnlargement {
				n.assignEntryToGroup(e, groupOne)
			} else if gOneEnlargement > gTwoEnlargement {
				n.assignEntryToGroup(e, groupTwo)
			} else if gOneRect.Area() < gTwoRect.Area() {
				n.assignEntryToGroup(e, groupOne)
			} else if gOneRect.Area() > gTwoRect.Area() {
				n.assignEntryToGroup(e, groupTwo)
			} else if len(groupOne.entries) <= len(groupTwo.entries) {
				n.assignEntryToGroup(e, groupOne)
			} else {
				n.assignEntryToGroup(e, groupTwo)
			}

		}

		otherEntries = append(otherEntries[:next], otherEntries[next+1:]...)
	}

	return groupOne, groupTwo
}

func (n *Node) assignEntryToGroup(e Entry, group *Node) {
	eChild, _ := n.Dal.GetNode(e.child)
	if eChild != nil {
		eChild.parent = group.PageNum
		n.Dal.WriteNode(eChild)
	}
	group.entries = append(group.entries, e)
	n.Dal.WriteNode(group)
}

func pickNext(groupOne, groupTwo *Node, entries []Entry) int {
	maxDiff := math.Inf(-1)
	var chosenEntry int
	gOneRect := createNodeRectangle(*groupOne)
	gTwoRect := createNodeRectangle(*groupTwo)

	for i, e := range entries {
		d1 := createRectangle(gOneRect, e.rect).Area() - gOneRect.Area()
		d2 := createRectangle(gTwoRect, e.rect).Area() - gTwoRect.Area()
		d := math.Abs(d1 - d2)
		if d > maxDiff {
			maxDiff = d
			chosenEntry = i
		}
	}
	return chosenEntry
}

func (tree *Rtree) Delete(obj SpatialData) bool {
	root, err := tree.Dal.GetNode(tree.root)
	if err != nil {
		panic(err)
	}

	n := tree.findLeaf(root, obj)
	if n == nil {
		return false
	}

	delIDx := -1
	for i, e := range n.entries {
		if bytes.Compare(e.obj.Data, obj.Data) == 0 {
			delIDx = i
			break
		}
	}

	if delIDx < 0 {
		return false
	}

	n.entries[delIDx] = n.entries[len(n.entries)-1]
	n.entries = n.entries[:len(n.entries)-1]

	tree.condenseTree(n)
	tree.size--

	tree.Dal.WriteNode(n)

	if !root.leaf && len(root.entries) == 1 {
		rEntryChild, err := tree.Dal.GetNode(root.entries[0].child)
		if err != nil {
			panic(err)
		}
		root = rEntryChild
		tree.Dal.WriteNode(root)
	}

	tree.height = root.level

	return true
}

func (tree *Rtree) findLeaf(n *Node, obj SpatialData) *Node {
	if n.leaf {
		return n
	}
	for _, e := range n.entries {
		if e.rect.containRect(obj.Bounds()) {
			eChild, _ := tree.Dal.GetNode(e.child)

			leaf := tree.findLeaf(eChild, obj)
			if leaf == nil {
				continue
			}

			for _, leafEntry := range leaf.entries {
				if bytes.Compare(leafEntry.obj.Data, obj.Data) == 0 {
					return leaf
				}
			}
		}
	}
	return nil
}

func (tree *Rtree) condenseTree(n *Node) {
	deleted := []types.Pgnum{}

	root, err := tree.Dal.GetNode(tree.root)
	if err != nil {
		panic(err)
	}

	for n != root {
		nParent, err := tree.Dal.GetNode(n.parent)
		if err != nil {
			panic(err)
		}

		if len(n.entries) < tree.MinEntries {
			idx := -1
			_, idx = n.getNFromParentEntry()

			if idx == -1 {
				panic(err)
			}
			l := len(nParent.entries)
			nParent.entries[idx] = nParent.entries[l-1]
			nParent.entries = nParent.entries[:l-1]

			deleted = append(deleted, n.PageNum)
		} else {
			en, idx := n.getNFromParentEntry()
			prevRect := en.rect
			en.rect = createNodeRectangle(*n)

			nParent.entries[idx] = *en
			if en.rect.Equal(prevRect) {
				break
			}
		}
		tree.Dal.WriteNode(nParent)
		n = nParent
	}

	for i := len(deleted) - 1; i >= 0; i-- {
		n := deleted[i]
		nNode, err := tree.Dal.GetNode(n)
		if err != nil {
			panic(err)
		}

		e := Entry{createNodeRectangle(*nNode), n, SpatialData{}}
		tree.insert(e, nNode.level+1)
	}
}

func (tree *Rtree) NearestNeighbors(k int, p Point) []SpatialData {
	tree.latch.RLock()

	nearestListsPQ := priorityQueue[SpatialData]{}

	root, err := tree.Dal.GetNode(tree.root)
	if err != nil {
		panic(err)
	}

	tree.nearestNeighbors(k, p, root, &nearestListsPQ)

	nearestLists := []SpatialData{}
	for nearestListsPQ.Len() > 0 {
		nearestLists = append(nearestLists, heap.Pop(&nearestListsPQ).(*priorityQueueNode[SpatialData]).item)
	}
	nearestLists = ReverseG(nearestLists)
	tree.latch.RUnlock()
	return nearestLists
}

func ReverseG[T any](arr []T) (result []T) {
	for i, j := 0, len(arr)-1; i < j; i, j = i+1, j-1 {
		arr[i], arr[j] = arr[j], arr[i]
	}
	return arr
}

type activeBranch struct {
	entry Entry
	Dist  float64
}

func sortActiveBranchLists(p Point, entries []Entry) ([]Entry, []float64) {

	sorted := make([]activeBranch, len(entries))
	for i := 0; i < len(entries); i++ {
		sorted[i] = activeBranch{entries[i], p.minDist(entries[i].rect)}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Dist < sorted[j].Dist
	})

	maxDist := sorted[len(sorted)-1].Dist

	removeIDx := len(sorted)
	for i := range sorted {
		if sorted[i].Dist > maxDist {
			removeIDx = i
			break
		}
	}

	sorted = sorted[:removeIDx]
	sortedEntries := make([]Entry, len(sorted))
	for i, e := range sorted {
		sortedEntries[i] = e.entry
	}
	dists := make([]float64, len(sorted))
	for i, e := range sorted {
		dists[i] = e.Dist
	}

	return sortedEntries, dists
}

func insertToNearestLists(nearestLists *priorityQueue[SpatialData], obj SpatialData, dist float64, k int) {
	if nearestLists.Len() < k {
		heap.Push(nearestLists, &priorityQueueNode[SpatialData]{rank: dist, item: obj})
	} else if dist < (*nearestLists)[0].rank {
		heap.Pop(nearestLists)
		heap.Push(nearestLists, &priorityQueueNode[SpatialData]{rank: dist, item: obj})
	}
}

func (tree *Rtree) nearestNeighbors(k int, q Point, n *Node, nearestLists *priorityQueue[SpatialData]) {
	var nearestListMaxDist float64 = math.Inf(1)

	pq := *nearestLists
	if nearestLists.Len() < k && nearestLists.Len() > 0 {
		nearestListMaxDist = pq[0].rank
	}

	qLoc := NewLocation(q.Lat, q.Lon)
	if n.leaf {
		for _, e := range n.entries {
			objectLoc := NewLocation(e.obj.Location.Lat, e.obj.Location.Lon)
			dist := HaversineDistance(qLoc, objectLoc)
			if dist < nearestListMaxDist {
				insertToNearestLists(nearestLists, e.obj, dist, k)
			}
		}

	} else {
		activeBranchLists := make([]Entry, len(n.entries))
		copy(activeBranchLists, n.entries)
		activeBranchLists, dists := sortActiveBranchLists(q, activeBranchLists)

		for i, e := range activeBranchLists {
			eChild, _ := n.Dal.GetNode(e.child)

			if dists[i] < nearestListMaxDist {
				tree.nearestNeighbors(k, q, eChild, nearestLists)
			} else {
				break // activeBranchList udah sorted
			}
		}

	}
}

func (tree *Rtree) Update(obj SpatialData, newObj SpatialData) error {
	found := tree.Delete(obj)
	if !found {
		return errors.New("object not found")
	}
	tree.Insert(newObj)
	return nil
}

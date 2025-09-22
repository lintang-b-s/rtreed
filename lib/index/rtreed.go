package index

import (
	"bytes"
	"container/heap"
	"errors"
	"math"
	"os"
	"sort"

	"github.com/lintang-b-s/lbs/lib"
	"github.com/lintang-b-s/lbs/lib/buffer"
	"github.com/lintang-b-s/lbs/lib/disk"
	"github.com/lintang-b-s/lbs/lib/log"
	"github.com/lintang-b-s/lbs/lib/meta"
	"github.com/lintang-b-s/lbs/lib/tree"
	"github.com/lintang-b-s/lbs/types"
)

type Rtreed struct {
	dim               int
	minEntries        int
	maxEntries        int
	bufferPoolManager BufferPoolManager
	diskManager       DiskManagerI
	logManager        LogManagerI
	root              types.BlockNum
	freeList          *meta.Freelist
	metadata          *meta.Meta
	size              int32
	height            int
}

func NewRtreed(dim, min, max int) (*Rtreed, error) {

	_, err := os.Stat(lib.DB_DIR)
	if !os.IsNotExist(err) {
		// db exists
		dm := disk.NewDiskManager(lib.DB_DIR, lib.MAX_PAGE_SIZE)
		lm, err := log.NewLogManager(dm, lib.LOG_FILE_NAME)
		if err != nil {
			panic(err)
		}
		bufferPoolManager := buffer.NewBufferPoolManager(lib.MAX_BUFFER_POOL_SIZE, dm, lm)

		rt := &Rtreed{
			dim:               dim,
			minEntries:        min,
			maxEntries:        max,
			diskManager:       dm,
			logManager:        lm,
			bufferPoolManager: bufferPoolManager,
		}

		meta, err := rt.readMeta()
		if err != nil {
			return nil, err
		}
		rt.metadata = meta

		freelist, err := rt.readFreeList()
		if err != nil {
			return nil, err
		}
		rt.freeList = freelist

		rt.root = rt.metadata.GetRoot()
		rt.height = rt.metadata.GetHeight()
		rt.size = rt.metadata.GetSize()
		return rt, nil

	} else if os.IsNotExist(err) {
		// db not exist, create new
		dm := disk.NewDiskManager(lib.DB_DIR, lib.MAX_PAGE_SIZE)
		lm, err := log.NewLogManager(dm, lib.LOG_FILE_NAME)
		if err != nil {
			panic(err)
		}
		bufferPoolManager := buffer.NewBufferPoolManager(lib.MAX_BUFFER_POOL_SIZE, dm, lm)

		rt := &Rtreed{
			dim:               dim,
			minEntries:        min,
			maxEntries:        max,
			diskManager:       dm,
			logManager:        lm,
			bufferPoolManager: bufferPoolManager,
			metadata:          meta.NewEmptyMeta(),
		}
		rt.freeList = meta.NewFreelist()

		rt.metadata.SetFreelistPage(rt.freeList.GetNextPage())
		rt.root = 2
		rt.metadata.SetRoot(rt.root)
		rt.metadata.SetHeight(rt.height)
		rt.metadata.SetSize(rt.size)
		err = rt.writeMeta()
		if err != nil {
			return nil, err
		}

		err = rt.writeFreeList()
		if err != nil {
			return nil, err
		}

		rootNode := tree.NewNode([]tree.Entry{}, 0, 1, true)
		rootNode, err = rt.writeNode(rootNode)
		if err != nil {
			return nil, err
		}

		return rt, nil
	}
	return nil, nil
}

func (rt *Rtreed) Insert(obj tree.SpatialData) {
	e := tree.NewEntry(obj.Bounds(), newPageNum, obj)
	rt.insert(e, 1)

	rt.size++
	rt.updateMetaHeightSeize(rt.height, rt.size)
}

type unpinPage struct {
	pageNum types.BlockNum
	isDirty bool
}

func (u *unpinPage) getPageNum() types.BlockNum {
	return u.pageNum
}

func (u *unpinPage) getIsDirty() bool {
	return u.isDirty
}

func newUnpinPage(pageNum types.BlockNum, isDirty bool) unpinPage {
	return unpinPage{pageNum, isDirty}
}

func (rt *Rtreed) insert(e tree.Entry, level int) {

	needToUnpin := make([]unpinPage, 0, 10)

	root, err := rt.getNode(rt.root)
	if err != nil {
		panic(err)
	}
	needToUnpin = append(needToUnpin, newUnpinPage(root.GetPageNum(), false))

	var leaf *tree.Node
	if level != 1 {
		leaf = rt.chooseNode(root, e, level, &needToUnpin)
	} else {
		leaf = rt.chooseLeaf(root, e, &needToUnpin)
	}

	leaf.AppendEntry(e)

	eChild, _ := rt.getNode(e.GetChild())
	needToUnpin = append(needToUnpin, newUnpinPage(e.GetChild(), false))

	if eChild != nil {
		eChild.SetParent(leaf.GetPageNum())
		if eChild.GetPageNum() == 0 {
			eChild.SetPageNum(1)
		}
		rt.writeNode(eChild)

		needToUnpin = append(needToUnpin, newUnpinPage(eChild.GetPageNum(), true))
	}

	leaf.SetEntry(len(leaf.GetEntries())-1, tree.NewEntry(e.GetRect(), eChild.GetPageNum(), e.GetObject()))
	leaf, err = rt.writeNode(leaf)
	if err != nil {
		panic(err)
	}
	needToUnpin = append(needToUnpin, newUnpinPage(leaf.GetPageNum(), true))
	leafIsRoot := leaf.GetPageNum() == root.GetPageNum()
	var ll *tree.Node
	if leaf.GetEntriesSize() > rt.maxEntries {
		leaf, ll = rt.splitNode(leaf, rt.minEntries, &needToUnpin)
	}

	l, ll := rt.adjustTree(leaf, ll, &needToUnpin, leafIsRoot)
	if ll != nil && l.GetPageNum() == root.GetPageNum() {
		oldRoot := l
		rt.height++

		newRoot := &tree.Node{}
		newRoot.SetPageNum(1)

		newRootEntries := make([]tree.Entry, 0, 2)
		newRootEntries = append(newRootEntries, tree.NewEntry(createNodeRectangle(*oldRoot), oldRoot.GetPageNum(), tree.SpatialData{}))
		newRootEntries = append(newRootEntries, tree.NewEntry(createNodeRectangle(*ll), ll.GetPageNum(), tree.SpatialData{}))

		newRoot.SetEntries(newRootEntries)
		newRoot.SetParent(0)
		newRoot.SetIsleaf(false)
		newRoot.SetLevel(rt.height)
		newRoot.SetEntries(newRootEntries)

		newRoot, err = rt.writeNode(newRoot)
		if err != nil {
			panic(err)
		}
		needToUnpin = append(needToUnpin, newUnpinPage(newRoot.GetPageNum(), true))

		rt.root = newRoot.GetPageNum()
		rt.upateMetaRoot(newRoot.GetPageNum())

		oldRoot.SetParent(rt.root)
		ll.SetParent(rt.root)
		rt.writeNode(ll)
		rt.writeNode(oldRoot)

		needToUnpin = append(needToUnpin, newUnpinPage(ll.GetPageNum(), true))
		needToUnpin = append(needToUnpin, newUnpinPage(oldRoot.GetPageNum(), true))
	}

	for _, p := range needToUnpin {
		blockId := disk.NewBlockID(lib.PAGE_FILE_NAME, int(p.getPageNum()))
		rt.bufferPoolManager.UnpinPage(blockId, p.getIsDirty())
	}
}

func chooseLeastEnlargement(entries []tree.Entry, e tree.Entry) types.BlockNum {
	rectAreaDiff := math.MaxFloat64
	var leastEnlEntry tree.Entry
	for _, en := range entries {
		rect := tree.CreateRectangle(en.GetRect(), e.GetRect())
		currDiff := rect.Area() - en.GetRect().Area()
		if currDiff < rectAreaDiff || (currDiff == rectAreaDiff && en.GetRect().Area() < leastEnlEntry.GetRect().Area()) {
			rectAreaDiff = currDiff
			leastEnlEntry = en
		}
	}
	return types.BlockNum(leastEnlEntry.GetChild())
}

// chooseNode finds the node at the specified level to which e should be added.
func (rt *Rtreed) chooseNode(n *tree.Node, e tree.Entry, level int,
	needToUnpin *[]unpinPage) *tree.Node {
	if n.Level() == level {
		return n
	}
	chosenChild := chooseLeastEnlargement(n.GetEntries(), e)

	if chosenChild == 0 {
		return n
	}
	child, err := rt.getNode(chosenChild)
	if err != nil {
		panic(err)
	}
	*needToUnpin = append(*needToUnpin, newUnpinPage(n.GetPageNum(), false))
	return rt.chooseNode(child, e, level, needToUnpin)
}

func (rt *Rtreed) chooseLeaf(n *tree.Node, e tree.Entry, needToUnpin *[]unpinPage) *tree.Node {

	if n.IsLeaf() {
		return n
	}

	chosenChild := chooseLeastEnlargement(n.GetEntries(), e)

	if chosenChild == 0 {
		return n
	}

	child, err := rt.getNode(chosenChild)
	if err != nil {
		panic(err)
	}
	*needToUnpin = append(*needToUnpin, newUnpinPage(n.GetPageNum(), false))
	return rt.chooseLeaf(child, e, needToUnpin)
}

func (rt *Rtreed) adjustTree(l, ll *tree.Node, needToUnpin *[]unpinPage, leafIsRoot bool) (*tree.Node, *tree.Node) {
	root, err := rt.getNode(rt.root)
	if err != nil {
		panic(err)
	}
	*needToUnpin = append(*needToUnpin, newUnpinPage(root.GetPageNum(), leafIsRoot))

	if l.GetPageNum() == root.GetPageNum() {
		rt.writeNode(l)
		*needToUnpin = append(*needToUnpin, newUnpinPage(l.GetPageNum(), true))
		return l, ll
	}

	en, idx := rt.getNFromParentEntry(l, needToUnpin)

	prevRect := en.GetRect()
	en.SetRect(createNodeRectangle(*l))
	en.SetChild(l.GetPageNum())

	lParent, err := rt.getNode(l.GetParent())
	if err != nil {
		panic(err)
	}
	lParent.SetEntry(idx, *en)

	if ll == nil {
		rt.writeNode(l)
		rt.writeNode(lParent)

		*needToUnpin = append(*needToUnpin, newUnpinPage(l.GetPageNum(), true))
		*needToUnpin = append(*needToUnpin, newUnpinPage(lParent.GetPageNum(), true))

		if en.GetRect().Equal(prevRect) {
			return root, nil
		}
		return rt.adjustTree(lParent, nil, needToUnpin, leafIsRoot)
	}

	ell := tree.NewEntry(createNodeRectangle(*ll), ll.GetPageNum(), tree.SpatialData{})
	lParent.AppendEntry(ell)

	rt.writeNode(l)
	rt.writeNode(ll)

	*needToUnpin = append(*needToUnpin, newUnpinPage(l.GetPageNum(), true))
	*needToUnpin = append(*needToUnpin, newUnpinPage(ll.GetPageNum(), true))

	if len(lParent.GetEntries()) < rt.maxEntries {
		return rt.adjustTree(lParent, nil, needToUnpin, leafIsRoot)
	}

	newl, newll := rt.splitNode(lParent, rt.minEntries, needToUnpin)
	return rt.adjustTree(newl, newll, needToUnpin, leafIsRoot)
}

func (rt *Rtreed) getNFromParentEntry(n *tree.Node, needToUnpin *[]unpinPage) (*tree.Entry, int) {
	var e *tree.Entry
	idx := -1
	nParent, err := rt.getNode(n.GetParent())
	if err != nil {
		panic(err)
	}
	*needToUnpin = append(*needToUnpin, newUnpinPage(nParent.GetPageNum(), false))
	for i := range nParent.GetEntries() {
		if nParent.GetEntries()[i].GetChild() == n.GetPageNum() {
			return &nParent.GetEntries()[i], i
		}
	}

	return e, idx
}

func createNodeRectangle(node tree.Node) tree.Rect {
	if len(node.GetEntries()) == 1 {
		return node.GetEntries()[0].GetRect()
	}

	rect := tree.CreateRectangle(node.GetEntries()[0].GetRect(), node.GetEntries()[1].GetRect())
	for i := 2; i < len(node.GetEntries()); i++ {
		e := node.GetEntries()[i]
		rect = tree.CreateRectangle(rect, e.GetRect())
	}
	return rect
}

func (rt *Rtreed) pickSeeds(n *tree.Node) (int, int) {
	var entryOneIDx, entryTwoIDx int
	maxD := math.Inf(-1)
	for i, e1 := range n.GetEntries() {
		for j := i + 1; j < len(n.GetEntries()); j++ {
			e2 := n.GetEntries()[j]
			areaJ := tree.CreateRectangle(e1.GetRect(), e2.GetRect()).Area()
			d := areaJ - e1.GetRect().Area() - e2.GetRect().Area()
			if d > maxD {
				maxD = d
				entryOneIDx = i
				entryTwoIDx = j
			}
		}
	}
	return entryOneIDx, entryTwoIDx
}

func (rt *Rtreed) splitNode(n *tree.Node, minGroupSize int, needToUnpin *[]unpinPage) (*tree.Node, *tree.Node) {
	entryOneIDx, entryTwoIDx := rt.pickSeeds(n)
	entryOne, entryTwo := n.GetEntry(entryOneIDx), n.GetEntry(entryTwoIDx)

	otherEntriesOne := append(n.GetEntries()[:entryOneIDx], n.GetEntries()[entryOneIDx+1:entryTwoIDx]...)
	otherEntriesTwo := n.GetEntries()[entryTwoIDx+1:]
	otherEntries := append(otherEntriesOne, otherEntriesTwo...)

	groupOne := n
	groupOne.SetEntries([]tree.Entry{entryOne})
	groupTwo := &tree.Node{}
	groupTwo.SetParent(n.GetParent())
	groupTwo.SetIsleaf(n.IsLeaf())
	groupTwo.SetLevel(n.Level())
	groupTwo.SetEntries([]tree.Entry{entryTwo})
	groupTwo.SetPageNum(1)

	groupTwo, err := rt.writeNode(groupTwo)
	if err != nil {
		panic(err)
	}
	*needToUnpin = append(*needToUnpin, newUnpinPage(groupTwo.GetPageNum(), true))

	entryTwoChild, err := rt.getNode(entryTwo.GetChild())
	if err != nil {
		panic(err)
	}

	entryOneChild, err := rt.getNode(entryOne.GetChild())
	if err != nil {
		panic(err)
	}

	*needToUnpin = append(*needToUnpin, newUnpinPage(entryTwo.GetChild(), false))
	*needToUnpin = append(*needToUnpin, newUnpinPage(entryOne.GetChild(), false))
	if entryTwoChild != nil {
		entryTwoChild.SetParent(groupTwo.GetPageNum())
	}
	if entryOneChild != nil {
		entryOneChild.SetParent(groupOne.GetPageNum())
	}

	rt.writeNode(entryTwoChild)
	rt.writeNode(entryOneChild)
	*needToUnpin = append(*needToUnpin, newUnpinPage(entryTwo.GetChild(), true))
	*needToUnpin = append(*needToUnpin, newUnpinPage(entryOne.GetChild(), true))

	for len(otherEntries) > 0 {
		next := pickNext(groupOne, groupTwo, otherEntries)
		e := otherEntries[next]

		if len(otherEntries)+len(groupOne.GetEntries()) <= minGroupSize {
			rt.assignEntryToGroup(e, groupOne)
		} else if len(otherEntries)+len(groupTwo.GetEntries()) <= minGroupSize {
			rt.assignEntryToGroup(e, groupTwo)
		} else {

			gOneRect := createNodeRectangle(*groupOne)
			gTwoRect := createNodeRectangle(*groupTwo)
			gOneEntryRect := tree.CreateRectangle(gOneRect, e.GetRect())
			gTwoEntryRect := tree.CreateRectangle(gTwoRect, e.GetRect())

			gOneEnlargement := gOneEntryRect.Area() - gOneRect.Area()
			gTwoEnlargement := gTwoEntryRect.Area() - gTwoRect.Area()
			if gOneEnlargement < gTwoEnlargement {
				rt.assignEntryToGroup(e, groupOne)
			} else if gOneEnlargement > gTwoEnlargement {
				rt.assignEntryToGroup(e, groupTwo)
			} else if gOneRect.Area() < gTwoRect.Area() {
				rt.assignEntryToGroup(e, groupOne)
			} else if gOneRect.Area() > gTwoRect.Area() {
				rt.assignEntryToGroup(e, groupTwo)
			} else if len(groupOne.GetEntries()) <= len(groupTwo.GetEntries()) {
				rt.assignEntryToGroup(e, groupOne)
			} else {
				rt.assignEntryToGroup(e, groupTwo)
			}
		}
		otherEntries = append(otherEntries[:next], otherEntries[next+1:]...)
	}

	return groupOne, groupTwo
}

func (rt *Rtreed) assignEntryToGroup(e tree.Entry, group *tree.Node) {
	eChild, _ := rt.getNode(e.GetChild())
	var childPageNum types.BlockNum = 0
	rt.bufferPoolManager.UnpinPage(disk.NewBlockID(lib.PAGE_FILE_NAME, int(e.GetChild())), false)
	if eChild != nil {
		eChild.SetParent(group.GetPageNum())
		rt.writeNode(eChild)
		childPageNum = eChild.GetPageNum()
		rt.bufferPoolManager.UnpinPage(disk.NewBlockID(lib.PAGE_FILE_NAME, int(e.GetChild())), true)
	}
	e.SetChild(childPageNum)
	group.AppendEntry(e)
}

func pickNext(groupOne, groupTwo *tree.Node, entries []tree.Entry) int {
	maxDiff := math.Inf(-1)
	var chosenEntry int
	gOneRect := createNodeRectangle(*groupOne)
	gTwoRect := createNodeRectangle(*groupTwo)

	for i, e := range entries {
		d1 := tree.CreateRectangle(gOneRect, e.GetRect()).Area() - gOneRect.Area()
		d2 := tree.CreateRectangle(gTwoRect, e.GetRect()).Area() - gTwoRect.Area()
		d := math.Abs(d1 - d2)
		if d > maxDiff {
			maxDiff = d
			chosenEntry = i
		}
	}
	return chosenEntry
}

func (rt *Rtreed) Delete(obj tree.SpatialData) bool {
	root, err := rt.getNode(rt.root)
	if err != nil {
		panic(err)
	}

	n := rt.findLeaf(root, obj)
	if n == nil {
		return false
	}

	delIDx := -1
	for i, e := range n.GetEntries() {
		eObj := e.GetObject()
		if bytes.Compare(eObj.Data(), obj.Data()) == 0 {
			delIDx = i
			break
		}
	}

	if delIDx < 0 {
		return false
	}

	n.SetEntry(delIDx, n.GetEntries()[len(n.GetEntries())-1])
	n.SetEntries(n.GetEntries()[:len(n.GetEntries())-1])
	needToUnpin := make([]unpinPage, 0, 10)
	rt.condenseTree(n, &needToUnpin)
	rt.size--

	rt.writeNode(n)

	if !root.IsLeaf() && root.GetEntriesSize() == 1 {
		rEntryChild, err := rt.getNode(root.GetEntries()[0].GetChild())
		if err != nil {
			panic(err)
		}
		root = rEntryChild
		rt.writeNode(root)
	}

	rt.metadata.SetHeight(root.Level())

	return true
}

func (rt *Rtreed) findLeaf(n *tree.Node, obj tree.SpatialData) *tree.Node {
	if n.IsLeaf() {
		return n
	}
	for _, e := range n.GetEntries() {

		if e.GetRect().ContainRect(obj.Bounds()) {
			eChild, _ := rt.getNode(e.GetChild())

			leaf := rt.findLeaf(eChild, obj)
			if leaf == nil {
				continue
			}

			for _, leafEntry := range leaf.GetEntries() {
				leafObj := leafEntry.GetObject()
				if bytes.Compare(leafObj.Data(), obj.Data()) == 0 {
					return leaf
				}
			}
		}
	}

	return nil
}

func (rt *Rtreed) condenseTree(n *tree.Node, needToUnpin *[]unpinPage) {
	deleted := []unpinPage{}

	root, err := rt.getNode(rt.root)
	if err != nil {
		panic(err)
	}

	for n != root {
		nParent, err := rt.getNode(n.GetParent())
		if err != nil {
			panic(err)
		}

		if n.GetEntriesSize() < rt.minEntries {
			idx := -1
			_, idx = rt.getNFromParentEntry(n, needToUnpin)

			if idx == -1 {
				panic(err)
			}
			l := nParent.GetEntriesSize()
			nParent.SetEntry(idx, nParent.GetEntries()[l-1])
			nParent.SetEntries(nParent.GetEntries()[:l-1])

			deleted = append(deleted, newUnpinPage(n.GetPageNum(), true))
		} else {
			en, idx := rt.getNFromParentEntry(n, needToUnpin)
			prevRect := en.GetRect()
			en.SetRect(createNodeRectangle(*n))

			nParent.SetEntry(idx, *en)
			if en.GetRect().Equal(prevRect) {
				break
			}
		}
		rt.writeNode(nParent)
		n = nParent
	}

	for i := len(deleted) - 1; i >= 0; i-- {
		n := deleted[i]
		nNode, err := rt.getNode(n.getPageNum())
		if err != nil {
			panic(err)
		}

		e := tree.NewEntry(createNodeRectangle(*nNode), n.getPageNum(), tree.SpatialData{})
		rt.insert(e, nNode.Level()+1)
	}
}

func (rt *Rtreed) NearestNeighbors(k int, p tree.Point) []tree.SpatialData {

	nearestListsPQ := priorityQueue[tree.SpatialData]{}

	root, err := rt.getNode(rt.root)
	if err != nil {
		panic(err)
	}

	rt.nearestNeighbors(k, p, root, &nearestListsPQ)

	nearestLists := []tree.SpatialData{}
	for nearestListsPQ.Len() > 0 {
		nearestLists = append(nearestLists, heap.Pop(&nearestListsPQ).(*priorityQueueNode[tree.SpatialData]).item)
	}
	nearestLists = reverseG(nearestLists)
	return nearestLists
}

func reverseG[T any](arr []T) (result []T) {
	for i, j := 0, len(arr)-1; i < j; i, j = i+1, j-1 {
		arr[i], arr[j] = arr[j], arr[i]
	}
	return arr
}

type activeBranch struct {
	entry tree.Entry
	Dist  float64
}

func sortActiveBranchLists(p tree.Point, entries []tree.Entry) ([]tree.Entry, []float64) {

	sorted := make([]activeBranch, len(entries))
	for i := 0; i < len(entries); i++ {
		sorted[i] = activeBranch{entries[i], p.MinDist(entries[i].GetRect())}
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
	sortedEntries := make([]tree.Entry, len(sorted))
	for i, e := range sorted {
		sortedEntries[i] = e.entry
	}
	dists := make([]float64, len(sorted))
	for i, e := range sorted {
		dists[i] = e.Dist
	}

	return sortedEntries, dists
}

func insertToNearestLists(nearestLists *priorityQueue[tree.SpatialData], obj tree.SpatialData, dist float64, k int) {
	if nearestLists.Len() < k {
		heap.Push(nearestLists, &priorityQueueNode[tree.SpatialData]{rank: dist, item: obj})
	} else if dist < (*nearestLists)[0].rank {
		heap.Pop(nearestLists)
		heap.Push(nearestLists, &priorityQueueNode[tree.SpatialData]{rank: dist, item: obj})
	}
}

func (rt *Rtreed) nearestNeighbors(k int, q tree.Point, n *tree.Node, nearestLists *priorityQueue[tree.SpatialData]) {
	var nearestListMaxDist float64 = math.Inf(1)

	pq := *nearestLists
	if nearestLists.Len() < k && nearestLists.Len() > 0 {
		nearestListMaxDist = pq[0].rank
	}

	if n.IsLeaf() {
		for _, e := range n.GetEntries() {
			eObj := e.GetObject()
			dist := haversineDistance(q.Lat, q.Lon, eObj.Location().Lat, eObj.Location().Lon)
			if dist < nearestListMaxDist {
				insertToNearestLists(nearestLists, e.GetObject(), dist, k)
			}
		}

	} else {
		activeBranchLists := make([]tree.Entry, n.GetEntriesSize())
		copy(activeBranchLists, n.GetEntries())
		activeBranchLists, dists := sortActiveBranchLists(q, activeBranchLists)

		for i, e := range activeBranchLists {
			eChild, _ := rt.getNode(e.GetChild())

			if dists[i] < nearestListMaxDist {
				rt.nearestNeighbors(k, q, eChild, nearestLists)
			} else {
				break // activeBranchList udah sorted
			}
		}

	}
}

func (rt *Rtreed) Update(obj tree.SpatialData, newObj tree.SpatialData) error {
	found := rt.Delete(obj)
	if !found {
		return errors.New("object not found")
	}
	rt.Insert(newObj)
	return nil
}

func (rt *Rtreed) SearchWithinRadius(p tree.Point, radius float64) []tree.SpatialData {

	upperRightLat, upperRightLon := getDestinationPoint(p.Lat, p.Lon, 45, radius)
	lowerLeftLat, lowerLeftLon := getDestinationPoint(p.Lat, p.Lon, 225, radius)

	bound := tree.NewRectFromBounds(lowerLeftLat, lowerLeftLon, upperRightLat, upperRightLon)
	return rt.searchWithinBound(bound)
}

func (rt *Rtreed) searchWithinBound(bound tree.Rect) []tree.SpatialData {
	results := []tree.SpatialData{}
	root, err := rt.getNodeByte(rt.root)
	if err != nil {
		panic(err)
	}
	return rt.search(root, bound, results)
}

func (rt *Rtreed) search(node *disk.NodeByte, bound tree.Rect,
	results []tree.SpatialData) []tree.SpatialData {

	// S1. [Search subtrees.] If T is not a leaf,
	// check each entry E to determine
	// whether E.I Overlaps S. For all overlapping entries, invoke Search on the tree
	// whose root node is pointed to by E.p
	if !node.IsLeaf() {

		node.ForEntries(func(e tree.Entry) {
			if e.GetRect().Overlaps(bound) {
				eChildNode, err := rt.getNodeByte(e.GetChild())
				if err != nil {
					panic(err)
				}
				results = rt.search(eChildNode, bound, results)
			}
		})
	} else {

		node.ForEntries(func(e tree.Entry) {
			if e.GetRect().Overlaps(bound) {
				// S2. [Search leaf node.] If T is a leaf, check
				// all entries E to determine whether E.I
				// Overlaps S. If so, E is a qualifying
				// record
				eObj := e.GetObject()
				results = append(results, eObj)
			}
		})
	}

	return results
}

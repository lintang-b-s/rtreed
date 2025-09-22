package index

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
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
	bufferPoolManager BufferPoolManager
	diskManager       DiskManagerI
	logManager        LogManagerI
	metadata          *meta.Meta
	root              types.BlockNum
	dim               int
	minEntries        int
	maxEntries        int
	size              int32
	height            int
}

func NewRtreed(dim, min, max, maxSpatialDataInBytes int) (*Rtreed, error) {
	// max_page_size =  max_page_size =   21 bytes + maxEntries * (10 + 48 + 8 + maxSpatialDataInBytes) bytes size  [see page.go SerializeNode()]
	var err error
	lib.MAX_PAGE_SIZE = 21 + max*(10+48+8+maxSpatialDataInBytes)
	lib.MAX_BUFFER_POOL_SIZE = lib.MAX_BUFFER_POOL_SIZE_IN_MB * 1024 * 1024 / lib.MAX_PAGE_SIZE
	lib.MAX_PAGE_SIZE, err = lib.CeilPageSize(lib.MAX_PAGE_SIZE)
	_, err = os.Stat(lib.DB_DIR)
	if !os.IsNotExist(err) {
		// db exists
		dm := disk.NewDiskManager(lib.DB_DIR, lib.MAX_PAGE_SIZE)
		lm, err := log.NewLogManager(dm, lib.LOG_FILE_NAME)
		if err != nil {
			panic(err)
		}
		bufferPoolManager := buffer.NewBufferPoolManager(lib.MAX_BUFFER_POOL_SIZE, dm, lm, 1)

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
		rt.bufferPoolManager.SetNextBlockId(rt.metadata.GetNextBlockId())

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
		bufferPoolManager := buffer.NewBufferPoolManager(lib.MAX_BUFFER_POOL_SIZE, dm, lm, 1)

		rt := &Rtreed{
			dim:               dim,
			minEntries:        min,
			maxEntries:        max,
			diskManager:       dm,
			logManager:        lm,
			bufferPoolManager: bufferPoolManager,
			metadata:          meta.NewEmptyMeta(),
		}

		rt.root = 1
		rt.metadata.SetRoot(rt.root)
		rt.metadata.SetHeight(rt.height)
		rt.metadata.SetSize(rt.size)
		err = rt.writeMeta()
		if err != nil {
			return nil, err
		}

		rootNode := tree.NewNode([]*tree.Entry{}, 0, 1, true)
		rootNode.SetPageNum(2) // initial root page num is 2
		rootNode, err = rt.writeRootNode(rootNode)
		if err != nil {
			return nil, err
		}
		rt.bufferPoolManager.UnpinPage(disk.NewBlockID(lib.PAGE_FILE_NAME, int(rootNode.GetPageNum())), true)

		return rt, nil
	}
	return nil, nil
}

func (rt *Rtreed) Insert(obj tree.SpatialData) {
	e := tree.NewEntry(obj.Bounds(), lib.NEW_PAGE_NUM, obj)
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

func (rt *Rtreed) insert(e *tree.Entry, level int) {

	needToUnpin := make([]unpinPage, 0, 10)

	root, rootPage, err := rt.getNodeAndPage(rt.root)
	if err != nil {
		panic(err)
	}
	needToUnpin = append(needToUnpin, newUnpinPage(root.GetPageNum(), false))

	var leaf *tree.Node
	var leafPage *buffer.Buffer
	if level != 1 {
		leaf, leafPage = rt.chooseNode(root, rootPage, e, level, &needToUnpin)
	} else {
		leaf, leafPage = rt.chooseLeaf(root, rootPage, e, &needToUnpin)
	}

	leaf.AppendEntry(e)

	if e.GetChild() != lib.NEW_PAGE_NUM {
		// set parent
		eChild, eChildPage, err := rt.getNodeAndPage(e.GetChild())
		if err != nil {
			panic(err)
		}
		eChild.SetParent(leaf.GetPageNum())
		eChildPage.SerializeNode(eChild)
		needToUnpin = append(needToUnpin, newUnpinPage(eChild.GetPageNum(), true))

		leaf.SetEntry(len(leaf.GetEntries())-1, tree.NewEntry(e.GetRect(), eChild.GetPageNum(), e.GetObject()))
		leafPage.SerializeNode(leaf)
		needToUnpin = append(needToUnpin, newUnpinPage(leaf.GetPageNum(), true))
	}

	leafPage.SerializeNode(leaf)
	needToUnpin = append(needToUnpin, newUnpinPage(leaf.GetPageNum(), true))

	leafIsRoot := leaf.GetPageNum() == root.GetPageNum()
	var ll *tree.Node
	var llPage *buffer.Buffer
	if leaf.GetEntriesSize() > rt.maxEntries {
		leafPage, llPage = rt.splitNode(leafPage, rt.minEntries, &needToUnpin)
	}

	rootPage, splitRootPage := rt.adjustTree(leafPage, llPage, &needToUnpin, leafIsRoot)

	root = rootPage.DeserializeNode()

	if splitRootPage != nil {
		ll = splitRootPage.DeserializeNode()
		oldRoot := root
		rt.height++

		newRoot := &tree.Node{}
		newRoot.SetPageNum(lib.NEW_PAGE_NUM)

		newRootEntries := make([]*tree.Entry, 0, 2)
		newRootEntries = append(newRootEntries, tree.NewEntry(createNodeRectangle(*oldRoot), oldRoot.GetPageNum(), tree.SpatialData{}))
		newRootEntries = append(newRootEntries, tree.NewEntry(createNodeRectangle(*ll), ll.GetPageNum(), tree.SpatialData{}))

		newRoot.SetEntries(newRootEntries)
		newRoot.SetParent(0)
		newRoot.SetIsleaf(false)
		newRoot.SetLevel(rt.height)
		newRoot.SetEntries(newRootEntries)

		newRootUpdated, err := rt.writeNode(newRoot)
		if err != nil {
			panic(err)
		}
		needToUnpin = append(needToUnpin, newUnpinPage(newRootUpdated.GetPageNum(), true))

		rt.root = newRootUpdated.GetPageNum()
		rt.upateMetaRoot(newRootUpdated.GetPageNum())

		oldRoot.SetParent(rt.root)
		ll.SetParent(rt.root)

		splitRootPage.SerializeNode(ll)
		rootPage.SerializeNode(oldRoot)

		needToUnpin = append(needToUnpin, newUnpinPage(ll.GetPageNum(), true))
		needToUnpin = append(needToUnpin, newUnpinPage(oldRoot.GetPageNum(), true))
	}

	for _, p := range needToUnpin {
		blockId := disk.NewBlockID(lib.PAGE_FILE_NAME, int(p.getPageNum()))
		rt.bufferPoolManager.UnpinPage(blockId, p.getIsDirty())
	}
}

func chooseLeastEnlargement(entries []*tree.Entry, e *tree.Entry) types.BlockNum {
	rectAreaDiff := math.MaxFloat64
	var leastEnlEntry *tree.Entry
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
func (rt *Rtreed) chooseNode(n *tree.Node, nPage *buffer.Buffer, e *tree.Entry, level int,
	needToUnpin *[]unpinPage) (*tree.Node, *buffer.Buffer) {
	if n.Level() == level {
		return n, nPage
	}
	chosenChild := chooseLeastEnlargement(n.GetEntries(), e)

	if chosenChild == 0 {
		return n, nPage
	}
	child, childPage, err := rt.getNodeAndPage(chosenChild)
	if err != nil {
		panic(err)
	}

	*needToUnpin = append(*needToUnpin, newUnpinPage(chosenChild, false))
	return rt.chooseNode(child, childPage, e, level, needToUnpin)
}

func (rt *Rtreed) chooseLeaf(n *tree.Node, nPage *buffer.Buffer, e *tree.Entry, needToUnpin *[]unpinPage) (*tree.Node, *buffer.Buffer) {

	if n.IsLeaf() {
		return n, nPage
	}

	chosenChild := chooseLeastEnlargement(n.GetEntries(), e)

	if chosenChild == 0 {
		return n, nPage
	}

	child, childPage, err := rt.getNodeAndPage(chosenChild)
	if err != nil {
		panic(err)
	}
	en, _ := rt.getNFromParentEntry(child, needToUnpin)
	if en == nil {
		fmt.Printf("debug")
	}

	*needToUnpin = append(*needToUnpin, newUnpinPage(chosenChild, false))
	return rt.chooseLeaf(child, childPage, e, needToUnpin)
}

func (rt *Rtreed) adjustTree(lPage, llPage *buffer.Buffer, needToUnpin *[]unpinPage, leafIsRoot bool) (*buffer.Buffer, *buffer.Buffer) {
	root, rootPage, err := rt.getNodeAndPage(rt.root)
	if err != nil {
		panic(err)
	}
	*needToUnpin = append(*needToUnpin, newUnpinPage(root.GetPageNum(), leafIsRoot))
	l := lPage.DeserializeNode()

	if l.GetPageNum() == root.GetPageNum() {

		lPage.SerializeNode(l)
		if llPage != nil {
			ll := llPage.DeserializeNode()
			llPage.SerializeNode(ll)
			*needToUnpin = append(*needToUnpin, newUnpinPage(ll.GetPageNum(), true))
		}
		*needToUnpin = append(*needToUnpin, newUnpinPage(l.GetPageNum(), true))
		return lPage, llPage
	}

	en, idx := rt.getNFromParentEntry(l, needToUnpin)

	prevRect := en.GetRect()
	en.SetRect(createNodeRectangle(*l))
	en.SetChild(l.GetPageNum())

	lParent, lParentPage, err := rt.getNodeAndPage(l.GetParent())
	if err != nil {
		panic(err)
	}
	lParent.SetEntry(idx, en)

	if llPage == nil {

		lPage.SerializeNode(l)
		lParentPage.SerializeNode(lParent)

		*needToUnpin = append(*needToUnpin, newUnpinPage(l.GetPageNum(), true))
		*needToUnpin = append(*needToUnpin, newUnpinPage(lParent.GetPageNum(), true))

		if en.GetRect().Equal(prevRect) {
			return rootPage, nil
		}
		return rt.adjustTree(lParentPage, nil, needToUnpin, leafIsRoot)
	}

	ll := llPage.DeserializeNode()

	ell := tree.NewEntry(createNodeRectangle(*ll), ll.GetPageNum(), tree.SpatialData{})
	lParent.AppendEntry(ell)

	llPage.SerializeNode(ll)
	lPage.SerializeNode(l)
	lParentPage.SerializeNode(lParent)

	*needToUnpin = append(*needToUnpin, newUnpinPage(lParent.GetPageNum(), true))
	*needToUnpin = append(*needToUnpin, newUnpinPage(l.GetPageNum(), true))
	*needToUnpin = append(*needToUnpin, newUnpinPage(ll.GetPageNum(), true))

	if len(lParent.GetEntries()) > rt.maxEntries {
		newl, newll := rt.splitNode(lParentPage, rt.minEntries, needToUnpin)

		return rt.adjustTree(newl, newll, needToUnpin, leafIsRoot)
	}
	return rt.adjustTree(lParentPage, nil, needToUnpin, leafIsRoot)

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
			return nParent.GetEntries()[i], i
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

func (rt *Rtreed) splitNode(nPage *buffer.Buffer, minGroupSize int, needToUnpin *[]unpinPage) (*buffer.Buffer, *buffer.Buffer) {
	n := nPage.DeserializeNode()
	entryOneIDx, entryTwoIDx := rt.pickSeeds(n)
	entryOne, entryTwo := n.GetEntry(entryOneIDx), n.GetEntry(entryTwoIDx)

	otherEntriesOne := append(n.GetEntries()[:entryOneIDx], n.GetEntries()[entryOneIDx+1:entryTwoIDx]...)
	otherEntriesTwo := n.GetEntries()[entryTwoIDx+1:]
	otherEntries := append(otherEntriesOne, otherEntriesTwo...)

	groupOne := n
	groupOne.SetEntries([]*tree.Entry{entryOne})
	groupTwo := &tree.Node{}
	groupTwo.SetParent(n.GetParent())
	groupTwo.SetIsleaf(n.IsLeaf())
	groupTwo.SetLevel(n.Level())
	groupTwo.SetEntries([]*tree.Entry{entryTwo})
	groupTwo.SetPageNum(lib.NEW_PAGE_NUM)

	groupTwoUpdated, groupTwoPage, err := rt.writeNodeAndGetPage(groupTwo)
	if err != nil {
		panic(err)
	}

	if entryTwo.GetChild() != lib.NEW_PAGE_NUM {
		entryTwoChild, entryTwoChildPage, err := rt.getNodeAndPage(entryTwo.GetChild())
		if err != nil {
			panic(err)
		}
		entryTwoChild.SetParent(groupTwoUpdated.GetPageNum())

		entryTwoChildPage.SerializeNode(entryTwoChild)

		*needToUnpin = append(*needToUnpin, newUnpinPage(entryTwo.GetChild(), true))

	}
	if entryOne.GetChild() != lib.NEW_PAGE_NUM {
		entryOneChild, entryOneChildPage, err := rt.getNodeAndPage(entryOne.GetChild())
		if err != nil {
			panic(err)
		}
		entryOneChild.SetParent(groupOne.GetPageNum())

		entryOneChildPage.SerializeNode(entryOneChild)
		*needToUnpin = append(*needToUnpin, newUnpinPage(entryOne.GetChild(), true))
	}

	for len(otherEntries) > 0 {
		next := pickNext(groupOne, groupTwoUpdated, otherEntries)
		e := otherEntries[next]

		if len(otherEntries)+len(groupOne.GetEntries()) <= minGroupSize {
			rt.assignEntryToGroup(e, groupOne, needToUnpin)
		} else if len(otherEntries)+len(groupTwoUpdated.GetEntries()) <= minGroupSize {
			rt.assignEntryToGroup(e, groupTwoUpdated, needToUnpin)
		} else {

			gOneRect := createNodeRectangle(*groupOne)
			gTwoRect := createNodeRectangle(*groupTwoUpdated)
			gOneEntryRect := tree.CreateRectangle(gOneRect, e.GetRect())
			gTwoEntryRect := tree.CreateRectangle(gTwoRect, e.GetRect())

			gOneEnlargement := gOneEntryRect.Area() - gOneRect.Area()
			gTwoEnlargement := gTwoEntryRect.Area() - gTwoRect.Area()
			if gOneEnlargement < gTwoEnlargement {
				rt.assignEntryToGroup(e, groupOne, needToUnpin)
			} else if gOneEnlargement > gTwoEnlargement {
				rt.assignEntryToGroup(e, groupTwoUpdated, needToUnpin)
			} else if gOneRect.Area() < gTwoRect.Area() {
				rt.assignEntryToGroup(e, groupOne, needToUnpin)
			} else if gOneRect.Area() > gTwoRect.Area() {
				rt.assignEntryToGroup(e, groupTwoUpdated, needToUnpin)
			} else if len(groupOne.GetEntries()) <= len(groupTwoUpdated.GetEntries()) {
				rt.assignEntryToGroup(e, groupOne, needToUnpin)
			} else {
				rt.assignEntryToGroup(e, groupTwoUpdated, needToUnpin)
			}
		}
		otherEntries = append(otherEntries[:next], otherEntries[next+1:]...)
	}

	nPage.SerializeNode(groupOne)

	groupTwoPage.SerializeNode(groupTwoUpdated)

	*needToUnpin = append(*needToUnpin, newUnpinPage(groupOne.GetPageNum(), true))
	*needToUnpin = append(*needToUnpin, newUnpinPage(groupTwoUpdated.GetPageNum(), true))
	return nPage, groupTwoPage
}

func (rt *Rtreed) assignEntryToGroup(e *tree.Entry, group *tree.Node, needToUnpin *[]unpinPage) {
	if e.GetChild() != lib.NEW_PAGE_NUM {
		eChild, eChildPage, _ := rt.getNodeAndPage(e.GetChild())
		var childPageNum types.BlockNum = 0

		if eChildPage != nil {
			eChild.SetParent(group.GetPageNum())

			eChildPage.SerializeNode(eChild)

			childPageNum = eChild.GetPageNum()
		}

		*needToUnpin = append(*needToUnpin, newUnpinPage(childPageNum, true))

		e.SetChild(childPageNum)
	}

	group.AppendEntry(e)
}

func pickNext(groupOne, groupTwo *tree.Node, entries []*tree.Entry) int {
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

			nParent.SetEntry(idx, en)
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
	entry *tree.Entry
	Dist  float64
}

func sortActiveBranchLists(p tree.Point, entries []*tree.Entry) ([]*tree.Entry, []float64) {

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
	sortedEntries := make([]*tree.Entry, len(sorted))
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
		activeBranchLists := make([]*tree.Entry, n.GetEntriesSize())
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
	return rt.searchWithinBoundStack(bound)
}

func (rt *Rtreed) searchWithinBound(bound tree.Rect) []tree.SpatialData {
	results := make([]tree.SpatialData, 0, 100)
	needToUnpin := make([]unpinPage, 0, 20)
	root, err := rt.getNodeByte(rt.root)
	if err != nil {
		panic(err)
	}
	needToUnpin = append(needToUnpin, newUnpinPage(rt.root, false))

	results = rt.search(root, bound, results, &needToUnpin)
	for _, p := range needToUnpin {
		blockId := disk.NewBlockID(lib.PAGE_FILE_NAME, int(p.getPageNum()))
		rt.bufferPoolManager.UnpinPage(blockId, p.getIsDirty())
	}
	return results
}

func (rt *Rtreed) search(node *disk.NodeByte, bound tree.Rect,
	results []tree.SpatialData, needToUnpin *[]unpinPage) []tree.SpatialData {

	if !node.IsLeaf() {
		node.ForEntriesOverlaps(bound, func(child types.BlockNum) {
			// S1. [Search subtrees.] If T is not a leaf,
			// check each entry E to determine
			// whether E.I Overlaps S. For all overlapping entries, invoke Search on the tree
			// whose root node is pointed to by E.p
			eChildNode, err := rt.getNodeByte(child)
			*needToUnpin = append(*needToUnpin, newUnpinPage(child, false))
			if err != nil {
				panic(err)
			}
			results = rt.search(eChildNode, bound, results, needToUnpin)
		}, nil)
	} else {

		node.ForEntriesOverlaps(bound, nil, func(lat, lon float64, data []byte) {
			// S2. [Search leaf node.] If T is a leaf, check
			// all entries E to determine whether E.I
			// Overlaps S. If so, E is a qualifying
			// record
			obj := tree.NewSpatialData(tree.NewPoint(lat, lon), data)
			results = append(results, obj)
		})
	}

	return results
}

func (rt *Rtreed) searchWithinBoundStack(bound tree.Rect) []tree.SpatialData {
	results := make([]tree.SpatialData, 0, 100)
	needToUnpin := make([]unpinPage, 0, 20)

	needToUnpin = append(needToUnpin, newUnpinPage(rt.root, false))

	results = rt.searchStack(bound, needToUnpin)

	return results
}

func (rt *Rtreed) searchStack(bound tree.Rect, needToUnpin []unpinPage) []tree.SpatialData {
	results := make([]tree.SpatialData, 0, 100)
	stack := make([]types.BlockNum, 0, 16)
	stack = append(stack, rt.root)

	for len(stack) > 0 {
		nPageNum := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		node, err := rt.getNodeByte(nPageNum)
		if err != nil {
			panic(err)
		}
		needToUnpin = append(needToUnpin, newUnpinPage(nPageNum, false))

		if !node.IsLeaf() {
			node.ForEntriesOverlaps(bound, func(child types.BlockNum) {
				// S1. [Search subtrees.] If T is not a leaf,
				// check each entry E to determine
				// whether E.I Overlaps S. For all overlapping entries, invoke Search on the tree
				// whose root node is pointed to by E.p
				stack = append(stack, child)
			}, nil)
		} else {

			node.ForEntriesOverlaps(bound, nil, func(lat, lon float64, data []byte) {
				// S2. [Search leaf node.] If T is a leaf, check
				// all entries E to determine whether E.I
				// Overlaps S. If so, E is a qualifying
				// record
				obj := tree.NewSpatialData(tree.NewPoint(lat, lon), data)
				results = append(results, obj)
			})
		}
	}

	for _, p := range needToUnpin {
		blockId := disk.NewBlockID(lib.PAGE_FILE_NAME, int(p.getPageNum()))
		rt.bufferPoolManager.UnpinPage(blockId, p.getIsDirty())
	}

	return results
}

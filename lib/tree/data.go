package tree

import (
	"github.com/lintang-b-s/rtreed/lib"
	"github.com/lintang-b-s/rtreed/types"
)

type Node struct {
	entries []*Entry // max_entries * max_size_of_entry
	parent  types.BlockNum
	pageNum types.BlockNum
	level   int
	leaf    bool
}

func NewNode(entries []*Entry, parent types.BlockNum, level int, leaf bool) *Node {
	return &Node{
		entries: entries,
		parent:  parent,
		level:   level,
		leaf:    leaf,
		pageNum: lib.NEW_PAGE_NUM,
	}
}

func (n *Node) SetIsleaf(leaf bool) {
	n.leaf = leaf
}

func (n *Node) SetLevel(level int) {
	n.level = level
}

func (n *Node) SetParent(parent types.BlockNum) {
	n.parent = parent
}

func (n *Node) SetEntries(entries []*Entry) {
	n.entries = entries
}

func (n *Node) SetEntry(idx int, e *Entry) {
	n.entries[idx] = e
}

func (n *Node) SetPageNum(pageNum types.BlockNum) {
	n.pageNum = pageNum
}

func (n *Node) GetEntries() []*Entry {
	return n.entries
}

func (n *Node) IsLeaf() bool {
	return n.leaf
}

func (n *Node) Level() int {
	return n.level
}

func (n *Node) ForEntries(handle func(entry *Entry)) {
	for _, e := range n.entries {
		handle(e)
	}
}

func (n *Node) GetEntriesSize() int {
	return len(n.entries)
}

func (n *Node) GetParent() types.BlockNum {
	return n.parent
}

func (n *Node) GetPageNum() types.BlockNum {
	return n.pageNum
}

func (n *Node) AppendEntry(e *Entry) {
	n.entries = append(n.entries, e)
}

func (n *Node) GetEntry(idx int) *Entry {
	return n.entries[idx]
}

type Entry struct {
	obj   SpatialData    // var(max_obj) size
	rect  Rect           // 32 bytes
	child types.BlockNum // 8 bytes
}

// var(max_obj) + 40 bytes

func NewEntry(r Rect, c types.BlockNum, o SpatialData) *Entry {
	return &Entry{
		rect:  r,
		child: c,
		obj:   o,
	}
}

func (n *Entry) GetChild() types.BlockNum {
	return n.child
}

func (n *Entry) GetObject() SpatialData {
	return n.obj
}

func (n *Entry) GetRect() Rect {
	return n.rect
}

func (n *Entry) SetChild(c types.BlockNum) {
	n.child = c
}

func (n *Entry) SetObject(o SpatialData) {
	n.obj = o
}

func (n *Entry) SetRect(r Rect) {
	n.rect = r
}

type SpatialData struct {
	location Point
	data     []byte
}

func NewSpatialData(p Point, d []byte) SpatialData {
	return SpatialData{location: p, data: d}
}

func (sd *SpatialData) Data() []byte {
	return sd.data
}

func (sd *SpatialData) SetData(d []byte) {
	sd.data = d
}

func (sd *SpatialData) Location() Point {
	return sd.location
}
func (sd *SpatialData) SetLocation(p Point) {
	sd.location = p
}

var tol = 0.0001

func (s *SpatialData) Bounds() Rect {
	return s.Location().ToRect(tol)
}

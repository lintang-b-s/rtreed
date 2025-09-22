package meta

import (
	"github.com/lintang-b-s/lbs/types"
)

const (
	metaPageNum = 0
)

type Meta struct {
	Root         types.BlockNum
	Height       int
	Size         int32
	freelistPage types.BlockNum
}

func (m *Meta) GetFreelistPage() types.BlockNum {
	return m.freelistPage
}

func (m *Meta) SetFreelistPage(p types.BlockNum) {
	m.freelistPage = p
}

func (m *Meta) GetRoot() types.BlockNum {
	return m.Root
}

func (m *Meta) SetRoot(r types.BlockNum) {
	m.Root = r
}

func (m *Meta) GetHeight() int {
	return m.Height
}

func (m *Meta) SetHeight(h int) {
	m.Height = h
}

func (m *Meta) GetSize() int32 {
	return m.Size
}

func (m *Meta) SetSize(s int32) {
	m.Size = s
}

func NewEmptyMeta() *Meta {
	return &Meta{}
}

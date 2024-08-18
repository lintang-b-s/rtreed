package page

import (
	"encoding/binary"

	"github.com/lintang-b-s/lbs/types"
)

const (
	metaPageNum = 0
)

type meta struct {
	Root         types.Pgnum
	Height       int
	Size         int32
	freelistPage types.Pgnum
}

func newEmptyMeta() *meta {
	return &meta{}
}

func (m *meta) serialize(buf []byte) {
	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.Root))
	pos += types.PageNumSize

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.freelistPage))
	pos += types.PageNumSize

	binary.LittleEndian.PutUint16(buf[pos:], uint16(m.Height))
	pos += 2

	binary.LittleEndian.PutUint32(buf[pos:], uint32(m.Size))
	pos += 4
}

func (m *meta) deserialize(buf []byte) {
	pos := 0

	m.Root = types.Pgnum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += types.PageNumSize

	m.freelistPage = types.Pgnum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += types.PageNumSize

	m.Height = int(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	m.Size = int32(binary.LittleEndian.Uint32(buf[pos:]))
	pos += 4
}

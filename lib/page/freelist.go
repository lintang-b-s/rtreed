package page

import (
	"encoding/binary"
	"sync"

	"github.com/lintang-b-s/lbs/types"
)

const metaPage = 0

type freelist struct {
	maxPage       types.Pgnum
	releasedPages []types.Pgnum
	latch         sync.Mutex
}

func newFreelist() *freelist {
	return &freelist{
		maxPage:       metaPage,
		releasedPages: []types.Pgnum{},
	}
}

func (fr *freelist) getNextPage() types.Pgnum {
	if len(fr.releasedPages) != 0 {

		pageID := fr.releasedPages[len(fr.releasedPages)-1]
		fr.releasedPages = fr.releasedPages[:len(fr.releasedPages)-1]
		return pageID
	}
	fr.latch.Lock()
	fr.maxPage += 1
	maxPage := fr.maxPage
	fr.latch.Unlock()

	return maxPage
}

func (fr *freelist) releasePage(page types.Pgnum) {
	fr.releasedPages = append(fr.releasedPages, page)
}

func (fr *freelist) serialize(buf []byte) []byte {
	pos := 0

	binary.LittleEndian.PutUint16(buf[pos:], uint16(fr.maxPage))
	pos += 2

	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(fr.releasedPages)))
	pos += 2

	for _, page := range fr.releasedPages {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(page))
		pos += types.PageNumSize

	}
	return buf
}

func (fr *freelist) deserialize(buf []byte) {
	pos := 0
	fr.maxPage = types.Pgnum(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	releasedPagesCount := int(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	for i := 0; i < releasedPagesCount; i++ {
		fr.releasedPages = append(fr.releasedPages, types.Pgnum(binary.LittleEndian.Uint64(buf[pos:])))
		pos += types.PageNumSize
	}
}

package meta

import (
	"sync"

	"github.com/lintang-b-s/lbs/types"
)

const metaPage = 0

type Freelist struct {
	maxPage       types.BlockNum
	releasedPages []types.BlockNum
	latch         sync.Mutex
}

func (fr *Freelist) SetMaxPage(maxPage types.BlockNum) {
	fr.maxPage = maxPage
}

func (fr *Freelist) SetReleasedPages(releasedPages []types.BlockNum) {
	fr.releasedPages = releasedPages
}

func (fr *Freelist) ReleasePage(page types.BlockNum) {
	fr.releasedPages = append(fr.releasedPages, page)
}

func (fr *Freelist) MaxPage() types.BlockNum {
	return fr.maxPage
}

func (fr *Freelist) ReleasedPages() []types.BlockNum {
	return fr.releasedPages
}

func NewFreelist() *Freelist {
	return &Freelist{
		maxPage:       metaPage,
		releasedPages: []types.BlockNum{},
	}
}

func (fr *Freelist) GetNextPage() types.BlockNum {
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

package disk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/lintang-b-s/lbs/lib/meta"
	"github.com/lintang-b-s/lbs/lib/tree"
	"github.com/lintang-b-s/lbs/types"
)

// Page . menyimpan data satu block page di dalam memori buffer (also disimpan di disk). (berukuran blockSize)
type Page struct {
	bb *bytes.Buffer
}

func NewPage(blockSize int) *Page {
	bb := bytes.NewBuffer(make([]byte, blockSize))
	return &Page{bb}
}

func NewPageFromByteSlice(b []byte) *Page {
	return &Page{bytes.NewBuffer(b)}
}

func (p *Page) GetInt(offset int32) int32 {
	return int32(binary.LittleEndian.Uint32(p.bb.Bytes()[offset:]))
}

// PutInt. set int ke byte array page di posisi = offset.
func (p *Page) PutInt(offset int32, val int32) {
	binary.LittleEndian.PutUint32(p.bb.Bytes()[offset:], uint32(val))
}

func (p *Page) PutUint16(offset int32, val uint16) {
	binary.LittleEndian.PutUint16(p.bb.Bytes()[offset:], val)
}

func (p *Page) GetUint16(offset int32) uint16 {
	return binary.LittleEndian.Uint16(p.bb.Bytes()[offset:])
}

func (p *Page) PutUint64(offset int32, val uint64) {
	binary.LittleEndian.PutUint64(p.bb.Bytes()[offset:], val)
}

func (p *Page) GetUint64(offset int32) uint64 {
	return binary.LittleEndian.Uint64(p.bb.Bytes()[offset:])
}

// GetBytes. return byte array dari byte array page di posisi = offset. di awal ada panjang bytes nya sehingga buat read bytes tinggal baca buffer page[offset+4:offset+4+length]
func (p *Page) GetBytes(offset int32) []byte {
	length := p.GetInt(offset)
	b := make([]byte, length)
	copy(b, p.bb.Bytes()[offset+4:offset+4+length])
	return b
}

// PutBytes. set byte array ke byte array page di posisi = offset.
func (p *Page) PutBytes(offset int32, b []byte) (int, error) {
	if offset+int32(len(b)) > int32(len(p.bb.Bytes())) {
		return 0, errors.New("put bytes out of bound")
	}
	p.PutInt(offset, int32(len(b)))
	copy(p.bb.Bytes()[offset+4:], b)
	return len(b) + 4, nil
}

// GetString. return string dari byte array page di posisi= offset.
func (p *Page) GetString(offset int32) string {
	return string(p.GetBytes(offset))
}

// putString. set string ke byte array page di posisi = offset.
func (p *Page) PutString(offset int32, s string) {
	p.PutBytes(offset, []byte(s))
}

func (p *Page) PutBool(offset int32, val bool) {
	var bitSetVar uint64
	if val {
		bitSetVar = 1
	}
	p.bb.Bytes()[offset] = byte(bitSetVar)
}

func (p *Page) GetBool(offset int32) bool {
	return p.bb.Bytes()[offset] == byte(1)
}

func (p *Page) Contents() []byte {
	return p.bb.Bytes()
}

func (p *Page) SerializeNode(node *tree.Node) {
	isLeaf := node.IsLeaf()
	p.PutBool(0, isLeaf)
	p.PutUint16(1, uint16(node.GetEntriesSize()))
	p.PutUint16(3, uint16(node.Level()))
	p.PutUint64(5, uint64(node.GetParent()))
	p.PutUint64(13, uint64(node.GetPageNum()))

	leftPos := int32(21)
	rightPos := len(p.bb.Bytes()) - 1
	node.ForEntries(func(entry tree.Entry) {

		childNode := entry.GetChild()

		p.PutUint64(leftPos, uint64(childNode))
		leftPos += types.PageNumSize

		enObj := entry.GetObject()
		sLen := len(enObj.Data())

		payloadSize := 8*6 + sLen + 4*2

		offset := rightPos - payloadSize

		p.PutUint16(leftPos, uint16(offset))
		leftPos += 2

		rightPos -= sLen + 4
		p.PutBytes(int32(rightPos), enObj.Data())

		rightPos -= 4
		p.PutInt(int32(rightPos), int32(sLen))

		rightPos -= 8
		p.PutUint64(int32(rightPos), math.Float64bits(entry.GetRect().GetSLon()))
		rightPos -= 8
		p.PutUint64(int32(rightPos), math.Float64bits(entry.GetRect().GetTLon()))

		rightPos -= 8
		p.PutUint64(int32(rightPos), math.Float64bits(entry.GetRect().GetSLat()))
		rightPos -= 8
		p.PutUint64(int32(rightPos), math.Float64bits(entry.GetRect().GetTLat()))

		var llat, llon float64

		llat = enObj.Location().Lat
		llon = enObj.Location().Lon
		rightPos -= 8
		p.PutUint64(int32(rightPos), math.Float64bits(llat))
		rightPos -= 8
		p.PutUint64(int32(rightPos), math.Float64bits(llon))

	})
}

func (p *Page) DeserializeNode() *tree.Node {
	node := &tree.Node{}
	isLeaf := p.GetBool(0)

	node.SetIsleaf(isLeaf)
	entriesCount := int(p.GetUint16(1))

	node.SetLevel(int(p.GetUint16(3)))
	node.SetParent(types.BlockNum(p.GetUint64(5)))
	node.SetPageNum(types.BlockNum(p.GetUint64(13)))

	entries := make([]tree.Entry, entriesCount)

	leftPos := int32(21)
	for i := 0; i < entriesCount; i++ {

		pageNum := types.BlockNum(p.GetUint64(leftPos))
		leftPos += types.PageNumSize
		entries[i].SetChild(pageNum)

		offset := p.GetUint16(leftPos)
		leftPos += 2

		locLon := math.Float64frombits(p.GetUint64(int32(offset)))
		offset += 8
		locLat := math.Float64frombits(p.GetUint64(int32(offset)))
		offset += 8

		rrect := &tree.Rect{}
		tLat := math.Float64frombits(p.GetUint64(int32(offset)))
		offset += 8
		SLat := math.Float64frombits(p.GetUint64(int32(offset)))
		offset += 8
		tLon := math.Float64frombits(p.GetUint64(int32(offset)))
		offset += 8
		sLon := math.Float64frombits(p.GetUint64(int32(offset)))
		offset += 8

		rrect.SetSLat(SLat)
		rrect.SetSLon(sLon)
		rrect.SetTLat(tLat)
		rrect.SetTLon(tLon)
		entries[i].SetRect(*rrect)

		sLen := p.GetInt(int32(offset))
		offset += 4

		spatialData := p.GetBytes(int32(offset))
		offset += uint16(sLen)

		entries[i].SetObject(tree.NewSpatialData(tree.NewPoint(locLat, locLon),
			spatialData))

	}

	node.SetEntries(entries)
	return node
}

func (p *Page) SerializeMetadata(m *meta.Meta) {
	leftPos := int32(0)
	p.PutUint64(leftPos, uint64(m.GetRoot()))
	leftPos += types.BlockNumSize

	p.PutUint64(leftPos, uint64(m.GetFreelistPage()))
	leftPos += types.BlockNumSize

	p.PutUint16(leftPos, uint16(m.GetHeight()))
	leftPos += 2

	p.PutInt(leftPos, int32(m.GetSize()))
	leftPos += 4
}

func (p *Page) DeserializeMetadata() *meta.Meta {
	m := meta.NewEmptyMeta()
	leftPos := int32(0)

	m.SetRoot(types.BlockNum(p.GetUint64(leftPos)))
	leftPos += types.BlockNumSize

	m.SetFreelistPage(types.BlockNum(p.GetUint64(leftPos)))
	leftPos += types.BlockNumSize

	m.SetHeight(int(p.GetUint16(leftPos)))
	leftPos += 2

	m.SetSize(int32(p.GetInt(leftPos)))
	leftPos += 4
	return m
}

func (p *Page) SerializeFreelist(fr *meta.Freelist) {
	leftPos := int32(0)

	p.PutUint16(leftPos, uint16(fr.MaxPage()))

	leftPos += 2

	p.PutUint16(leftPos, uint16(len(fr.ReleasedPages())))
	leftPos += 2

	for _, page := range fr.ReleasedPages() {
		p.PutUint64(leftPos, uint64(page))
		leftPos += types.BlockNumSize
	}
}

func (p *Page) DeserializeFreelist() *meta.Freelist {
	fr := meta.NewFreelist()
	leftPos := int32(0)
	fr.SetMaxPage(types.BlockNum(p.GetUint16(leftPos)))
	leftPos += 2

	releasedPagesCount := int(p.GetUint16(leftPos))
	leftPos += 2

	releasedPages := make([]types.BlockNum, releasedPagesCount)
	for i := 0; i < releasedPagesCount; i++ {
		releasedPages[i] = types.BlockNum(p.GetUint64(leftPos))
		leftPos += types.BlockNumSize
	}
	fr.SetReleasedPages(releasedPages)
	return fr
}

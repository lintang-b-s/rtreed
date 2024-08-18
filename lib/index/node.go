package index

import (
	"encoding/binary"
	"math"

	"github.com/lintang-b-s/lbs/types"
)

func NewEmptyNode() *Node {
	return &Node{}
}

func NewNodeForSerialization(entries []Entry, parent types.Pgnum, level int, leaf bool) *Node {
	return &Node{
		entries: entries,
		parent:  parent,
		level:   level,
		leaf:    leaf,
	}
}

func (t *Node) Serialize(buf []byte) []byte {
	leftPos := 0
	rightPos := len(buf) - 1

	isLeaf := t.leaf
	var bitSetVar uint64
	if isLeaf {
		bitSetVar = 1
	}
	buf[leftPos] = byte(bitSetVar)
	leftPos += 1

	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(len(t.entries)))
	leftPos += 2

	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(t.level))
	leftPos += 2

	binary.LittleEndian.PutUint64(buf[leftPos:], uint64(t.parent))
	leftPos += types.PageNumSize

	for eIDx := range t.entries {
		entry := t.entries[eIDx]
		if !isLeaf {
			childNode := entry.child

			binary.LittleEndian.PutUint64(buf[leftPos:], uint64(childNode))
			leftPos += types.PageNumSize
		}

		boundLen := (8 + 8) * 2

		sLen := len(entry.obj.Data)

		pLen := 8 + 8

		offset := rightPos - pLen - boundLen - sLen - 3
		binary.LittleEndian.PutUint16(buf[leftPos:], uint16(offset))
		leftPos += 2

		rightPos -= sLen
		copy(buf[rightPos:], entry.obj.Data)

		rightPos -= 1
		buf[rightPos] = byte(sLen)

		rightPos -= 8
		binary.LittleEndian.PutUint64(buf[rightPos:], math.Float64bits(entry.rect.s.Lon))
		rightPos -= 8
		binary.LittleEndian.PutUint64(buf[rightPos:], math.Float64bits(entry.rect.t.Lon))

		rightPos -= 8
		binary.LittleEndian.PutUint64(buf[rightPos:], math.Float64bits(entry.rect.s.Lat))
		rightPos -= 8
		binary.LittleEndian.PutUint64(buf[rightPos:], math.Float64bits(entry.rect.t.Lat))

		rightPos -= 1
		buf[rightPos] = byte(boundLen)

		rightPos -= 8
		var llat, llon float64

		if entry.obj.Location.Lat == 0 && entry.obj.Location.Lon == 0 {
			llat = 0
			llon = 0
		} else {
			llat = entry.obj.Location.Lat
			llon = entry.obj.Location.Lon
		}
		binary.LittleEndian.PutUint64(buf[rightPos:], math.Float64bits(llat))
		rightPos -= 8
		binary.LittleEndian.PutUint64(buf[rightPos:], math.Float64bits(llon))

		rightPos -= 1
		buf[rightPos] = byte(pLen)
	}

	return buf
}

func (t *Node) Deserialize(buf []byte) {

	leftPos := 0

	// Read header
	isLeaf := uint16(buf[0])
	t.leaf = isLeaf == 1

	entriesCount := int(binary.LittleEndian.Uint16(buf[1:3]))
	leftPos += 3

	level := int(binary.LittleEndian.Uint16(buf[3:5]))
	leftPos += 2
	t.level = level

	parent := types.Pgnum(binary.LittleEndian.Uint64(buf[5:13]))
	leftPos += types.PageNumSize
	t.parent = parent

	t.entries = make([]Entry, entriesCount)

	for i := 0; i < entriesCount; i++ {
		if isLeaf == 0 {
			pageNum := types.Pgnum(binary.LittleEndian.Uint64(buf[leftPos:]))
			leftPos += types.PageNumSize

			t.entries[i].child = pageNum
		}

		offset := binary.LittleEndian.Uint16(buf[leftPos:])
		leftPos += 2

		_ = uint16(buf[offset])
		offset += 1

		locLon := math.Float64frombits(binary.LittleEndian.Uint64(buf[offset : offset+8]))
		offset += 8

		locLat := math.Float64frombits(binary.LittleEndian.Uint64(buf[offset : offset+8]))
		offset += 8

		t.entries[i].obj.Location.Lon = locLon
		t.entries[i].obj.Location.Lat = locLat

		_ = uint16(buf[offset])
		offset += 1

		rrect := Rect{}

		rrect.t.Lat = math.Float64frombits(binary.LittleEndian.Uint64(buf[offset : offset+8]))
		offset += 8

		rrect.s.Lat = math.Float64frombits(binary.LittleEndian.Uint64(buf[offset : offset+8]))
		offset += 8

		rrect.t.Lon = math.Float64frombits(binary.LittleEndian.Uint64(buf[offset : offset+8]))
		offset += 8

		rrect.s.Lon = math.Float64frombits(binary.LittleEndian.Uint64(buf[offset : offset+8]))
		offset += 8

		t.entries[i].rect = rrect

		sLen := uint16(buf[offset])
		offset += 1

		spatialData := buf[offset : offset+sLen]
		offset += sLen
		t.entries[i].obj.Data = spatialData
	}

}

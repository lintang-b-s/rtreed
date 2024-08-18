package page

import (
	"errors"
	"fmt"
	"os"

	"github.com/lintang-b-s/lbs/lib/index"
	"github.com/lintang-b-s/lbs/types"
)

type Options struct {
	PageSize int

	MinFillPercent float32
	MaxFillPercent float32
}

var DefaultOptions = &Options{
	MinFillPercent: 0.5,
	MaxFillPercent: 0.95,
}

type page struct {
	num  types.Pgnum
	data []byte
}

type dal struct {
	pageSize       int
	minFillPercent float32
	maxFillPercent float32
	file           *os.File

	*meta
	*freelist
}

func NewDal(path string, options *Options) (*dal, error) {
	dal := &dal{
		meta:           newEmptyMeta(),
		pageSize:       options.PageSize,
		minFillPercent: options.MinFillPercent,
		maxFillPercent: options.MaxFillPercent,
	}

	if _, err := os.Stat(path); err == nil {
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.Close()
			return nil, err
		}

		meta, err := dal.readMeta()
		if err != nil {
			return nil, err
		}
		dal.meta = meta

		freelist, err := dal.readFreelist()
		if err != nil {
			return nil, err
		}
		dal.freelist = freelist

	} else if errors.Is(err, os.ErrNotExist) {

		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.Close()
			return nil, err
		}

		dal.freelist = newFreelist()
		dal.freelistPage = dal.getNextPage()
		_, err := dal.writeFreelist()
		if err != nil {
			return nil, err
		}

		collectionsNode, err := dal.WriteNode(index.NewNodeForSerialization([]index.Entry{}, 0, 1, true))
		if err != nil {
			return nil, err
		}
		dal.Root = collectionsNode.PageNum

		_, err = dal.writeMeta(dal.meta)
		return dal, err
	} else {
		return nil, err
	}
	return dal, nil
}

func (d *dal) Close() error {
	if d.file != nil {

		d.writeMeta(d.meta)
		err := d.file.Sync()
		if err != nil {
			fmt.Println(err.Error())
			panic(err)
		}
		err = d.file.Close()
		if err != nil {
			return fmt.Errorf("could not close file: %s", err)
		}
		d.file = nil
	}

	return nil
}

func (d *dal) allocateEmptyPage() *page {
	return &page{
		data: make([]byte, d.pageSize, d.pageSize),
	}
}

func (d *dal) readPage(pageNum types.Pgnum) (*page, error) {
	p := d.allocateEmptyPage()

	offset := int(pageNum) * d.pageSize
	_, err := d.file.ReadAt(p.data, int64(offset))
	if err != nil {
		return nil, err
	}
	return p, err
}

func (d *dal) writePage(p *page) error {
	offset := int64(p.num) * int64(d.pageSize)
	_, err := d.file.WriteAt(p.data, offset)
	return err
}

func (d *dal) GetNode(pageNum types.Pgnum) (*index.Node, error) {
	p, err := d.readPage(pageNum)
	if err != nil {
		return nil, err
	}
	node := index.NewEmptyNode()
	node.Deserialize(p.data)
	node.PageNum = pageNum
	node.Dal = d
	return node, nil
}

func (d *dal) WriteNode(n *index.Node) (*index.Node, error) {
	p := d.allocateEmptyPage()
	if n.PageNum == 0 {
		p.num = d.getNextPage()
		n.PageNum = p.num
	} else {
		p.num = n.PageNum
	}

	p.data = n.Serialize(p.data)

	err := d.writePage(p)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (d *dal) deleteNode(pageNum types.Pgnum) {
	d.releasePage(pageNum)
}

func (d *dal) readFreelist() (*freelist, error) {
	p, err := d.readPage(d.freelistPage)
	if err != nil {
		return nil, err
	}

	freelist := newFreelist()
	freelist.deserialize(p.data)
	return freelist, nil
}

func (d *dal) writeFreelist() (*page, error) {
	p := d.allocateEmptyPage()
	p.num = d.freelistPage
	d.freelist.serialize(p.data)

	err := d.writePage(p)
	if err != nil {
		return nil, err
	}
	d.freelistPage = p.num
	return p, nil
}

func (d *dal) writeMeta(meta *meta) (*page, error) {
	p := d.allocateEmptyPage()
	p.num = metaPageNum
	meta.serialize(p.data)

	err := d.writePage(p)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (d *dal) readMeta() (*meta, error) {
	p, err := d.readPage(metaPageNum)
	if err != nil {
		return nil, err
	}

	meta := newEmptyMeta()
	meta.deserialize(p.data)
	return meta, nil
}

func (d *dal) UpdateMetaHeightSize(height int, size int32) {
	d.meta.Height = height
	d.meta.Size = size
}

func (d *dal) UpdateMetaRoot(root types.Pgnum) {
	d.meta.Root = root
}

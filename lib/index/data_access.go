package index

import (
	"github.com/lintang-b-s/lbs/lib"
	"github.com/lintang-b-s/lbs/lib/disk"
	"github.com/lintang-b-s/lbs/lib/meta"
	"github.com/lintang-b-s/lbs/lib/tree"
	"github.com/lintang-b-s/lbs/types"
)

func (rt *Rtreed) getNode(pageNum types.BlockNum) (*tree.Node, error) {
	page, err := rt.bufferPoolManager.FetchPage(disk.NewBlockID(lib.PAGE_FILE_NAME, int(pageNum)))

	if err != nil {
		return nil, err
	}

	node := page.DeserializeNode()

	return node, nil
}

func (rt *Rtreed) getNodeByte(pageNum types.BlockNum) (*disk.NodeByte, error) {
	page, err := rt.bufferPoolManager.FetchPage(disk.NewBlockID(lib.PAGE_FILE_NAME, int(pageNum)))

	if err != nil {
		return nil, err
	}


	return page.GetNodePage(), nil
}

func (rt *Rtreed) writeNode(n *tree.Node) (*tree.Node, error) {
	var (
		page    *disk.Page
		err     error
		blockId disk.BlockID
	)
	if n.GetPageNum() == 1 {
		pageNum := rt.freeList.GetNextPage()
		n.SetPageNum(pageNum)
		page = disk.NewPage(lib.MAX_PAGE_SIZE)
		blockId = disk.NewBlockID(lib.PAGE_FILE_NAME, int(n.GetPageNum()))
		page.SerializeNode(n)
		err = rt.diskManager.Write(blockId, page)
		if err != nil {
			return nil, err
		}
		return n, nil
	} else {
		blockId = disk.NewBlockID(lib.PAGE_FILE_NAME, int(n.GetPageNum()))
		page, err = rt.bufferPoolManager.FetchPage(blockId)
		if err != nil {
			return nil, err
		}
		page.SerializeNode(n)

		return n, nil
	}

}

func (rt *Rtreed) writeFreeList() error {
	pageNum := rt.metadata.GetFreelistPage()
	blockId := disk.NewBlockID(lib.PAGE_FILE_NAME, int(pageNum))
	page := disk.NewPage(lib.MAX_PAGE_SIZE)

	page.SerializeFreelist(rt.freeList)
	err := rt.diskManager.Write(blockId, page)
	return err
}

func (rt *Rtreed) readFreeList() (*meta.Freelist, error) {
	pageNum := rt.metadata.GetFreelistPage()
	blockId := disk.NewBlockID(lib.PAGE_FILE_NAME, int(pageNum))
	page, err := rt.bufferPoolManager.FetchPage(blockId)
	if err != nil {
		return nil, err
	}

	freeList := page.DeserializeFreelist()
	return freeList, nil
}

func (rt *Rtreed) writeMeta() error {
	blockId := disk.NewBlockID(lib.PAGE_FILE_NAME, metaPageNum)
	page := disk.NewPage(lib.MAX_PAGE_SIZE)
	page.SerializeMetadata(rt.metadata)
	err := rt.diskManager.Write(blockId, page)
	return err
}

func (rt *Rtreed) readMeta() (*meta.Meta, error) {
	blockId := disk.NewBlockID(lib.PAGE_FILE_NAME, metaPageNum)
	page, err := rt.bufferPoolManager.FetchPage(blockId)
	if err != nil {
		return nil, err
	}

	metadata := page.DeserializeMetadata()
	return metadata, nil
}
func (rt *Rtreed) updateMetaHeightSeize(height int, size int32) {
	rt.metadata.SetHeight(height)
	rt.metadata.SetSize(size)
}

func (d *Rtreed) upateMetaRoot(rootPageNum types.BlockNum) {
	d.metadata.SetRoot(rootPageNum)
}
func (d *Rtreed) Close() error {

	err := d.writeMeta()
	if err != nil {
		return err
	}

	err = d.writeFreeList()
	if err != nil {
		return err
	}

	err = d.bufferPoolManager.FlushAll()
	if err != nil {
		return err
	}
	d.bufferPoolManager.Close()
	return nil
}

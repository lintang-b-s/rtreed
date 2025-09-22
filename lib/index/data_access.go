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

func (rt *Rtreed) getNodeAndPage(pageNum types.BlockNum) (*tree.Node, *disk.Page, error) {
	page, err := rt.bufferPoolManager.FetchPage(disk.NewBlockID(lib.PAGE_FILE_NAME, int(pageNum)))

	if err != nil {
		return nil, nil, err
	}

	node := page.DeserializeNode()

	return node, page, nil
}

func (rt *Rtreed) getNodeByte(pageNum types.BlockNum) (*disk.NodeByte, error) {
	page, err := rt.bufferPoolManager.FetchPage(disk.NewBlockID(lib.PAGE_FILE_NAME, int(pageNum)))

	if err != nil {
		return nil, err
	}

	return page.GetNodePage(), nil
}

func (rt *Rtreed) writeRootNode(n *tree.Node) (*tree.Node, error) {
	var (
		page    *disk.Page
		err     error
		blockId disk.BlockID
	)
	page, err = rt.bufferPoolManager.NewPage(&blockId)
	if err != nil {
		return nil, err
	}
	blockId.SetFileName(lib.PAGE_FILE_NAME)

	n.SetPageNum(types.BlockNum(blockId.GetBlockNum()))
	page.SerializeNode(n)
	err = rt.diskManager.Write(blockId, page)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (rt *Rtreed) writeNode(n *tree.Node) (*tree.Node, error) {
	var (
		page    *disk.Page
		err     error
		blockId disk.BlockID
	)
	if n.GetPageNum() == 3 {
		page, err = rt.bufferPoolManager.NewPage(&blockId)
		if err != nil {
			return nil, err
		}
		blockId.SetFileName(lib.PAGE_FILE_NAME)

		n.SetPageNum(types.BlockNum(blockId.GetBlockNum()))
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

func (rt *Rtreed) writeNodeAndGetPage(n *tree.Node) (*tree.Node, *disk.Page, error) {
	var (
		page    *disk.Page
		err     error
		blockId disk.BlockID
	)
	if n.GetPageNum() == 3 {
		page, err := rt.bufferPoolManager.NewPage(&blockId)
		if err != nil {
			return nil, nil, err
		}
		blockId.SetFileName(lib.PAGE_FILE_NAME)
		
		page.SerializeNode(n)
		n.SetPageNum(types.BlockNum(blockId.GetBlockNum()))
		err = rt.diskManager.Write(blockId, page)
		if err != nil {
			return nil, nil, err
		}
		return n, page, nil
	} else {
		blockId = disk.NewBlockID(lib.PAGE_FILE_NAME, int(n.GetPageNum()))
		page, err = rt.bufferPoolManager.FetchPage(blockId)
		if err != nil {
			return nil, nil, err
		}
		page.SerializeNode(n)

		return n, page, nil
	}

}

func (rt *Rtreed) writeFreeList() error {
	var blockId disk.BlockID
	page, err := rt.bufferPoolManager.NewPage(&blockId)
	if err != nil {
		return err
	}
	page.SerializeFreelist(rt.freeList)
	err = rt.diskManager.Write(blockId, page)
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
	var blockId disk.BlockID
	page, err := rt.bufferPoolManager.NewPage(&blockId)
	if err != nil {
		return err
	}
	page.SerializeMetadata(rt.metadata)
	err = rt.diskManager.Write(blockId, page)
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

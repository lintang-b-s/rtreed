package index

import (
	"github.com/lintang-b-s/rtreed/lib"
	"github.com/lintang-b-s/rtreed/lib/buffer"
	"github.com/lintang-b-s/rtreed/lib/disk"
	"github.com/lintang-b-s/rtreed/lib/meta"
	"github.com/lintang-b-s/rtreed/lib/tree"
	"github.com/lintang-b-s/rtreed/types"
)

func (rt *Rtreed) getNode(pageNum types.BlockNum) (*tree.Node, error) {
	buffer, err := rt.bufferPoolManager.FetchPage(disk.NewBlockID(lib.PAGE_FILE_NAME, int(pageNum)))

	if err != nil {
		return nil, err
	}

	node := buffer.DeserializeNode()

	return node, nil
}

func (rt *Rtreed) getNodeAndPage(pageNum types.BlockNum) (*tree.Node, *buffer.Buffer, error) {
	buffer, err := rt.bufferPoolManager.FetchPage(disk.NewBlockID(lib.PAGE_FILE_NAME, int(pageNum)))

	if err != nil {
		return nil, nil, err
	}

	node := buffer.DeserializeNode()

	return node, buffer, nil
}

func (rt *Rtreed) getNodeByte(pageNum types.BlockNum) (*disk.NodeByte, error) {
	buffer, err := rt.bufferPoolManager.FetchPage(disk.NewBlockID(lib.PAGE_FILE_NAME, int(pageNum)))

	if err != nil {
		return nil, err
	}

	return buffer.GetNodePage(), nil
}

func (rt *Rtreed) writeRootNode(n *tree.Node) (*tree.Node, error) {
	var (
		err     error
		blockId disk.BlockID
	)
	buffer, err := rt.bufferPoolManager.NewPage(&blockId)
	if err != nil {
		return nil, err
	}
	blockId.SetFileName(lib.PAGE_FILE_NAME)

	n.SetPageNum(types.BlockNum(blockId.GetBlockNum()))
	buffer.SerializeNode(n)
	err = rt.diskManager.Write(blockId, buffer.GetContents())
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (rt *Rtreed) writeNode(n *tree.Node) (*tree.Node, error) {
	var (
		blockId disk.BlockID
	)
	if n.GetPageNum() == lib.NEW_PAGE_NUM {
		buffer, err := rt.bufferPoolManager.NewPage(&blockId)
		if err != nil {
			return nil, err
		}
		blockId.SetFileName(lib.PAGE_FILE_NAME)

		n.SetPageNum(types.BlockNum(blockId.GetBlockNum()))
		buffer.SerializeNode(n)
		err = rt.diskManager.Write(blockId, buffer.GetContents())
		if err != nil {
			return nil, err
		}
		return n, nil
	} else {
		blockId = disk.NewBlockID(lib.PAGE_FILE_NAME, int(n.GetPageNum()))
		buffer, err := rt.bufferPoolManager.FetchPage(blockId)
		if err != nil {
			return nil, err
		}
		buffer.SerializeNode(n)

		return n, nil
	}

}

func (rt *Rtreed) writeNodeAndGetPage(n *tree.Node) (*tree.Node, *buffer.Buffer, error) {
	var (
		blockId disk.BlockID
	)
	if n.GetPageNum() == lib.NEW_PAGE_NUM {
		buffer, err := rt.bufferPoolManager.NewPage(&blockId)
		if err != nil {
			return nil, nil, err
		}
		blockId.SetFileName(lib.PAGE_FILE_NAME)
		n.SetPageNum(types.BlockNum(blockId.GetBlockNum()))

		buffer.SerializeNode(n)
		err = rt.diskManager.Write(blockId, buffer.GetContents())
		if err != nil {
			return nil, nil, err
		}
		return n, buffer, nil
	} else {
		blockId = disk.NewBlockID(lib.PAGE_FILE_NAME, int(n.GetPageNum()))
		buffer, err := rt.bufferPoolManager.FetchPage(blockId)
		if err != nil {
			return nil, nil, err
		}
		buffer.SerializeNode(n)

		return n, buffer, nil
	}

}

func (rt *Rtreed) writeMeta() error {
	page := disk.NewPage(lib.MAX_PAGE_SIZE)
	blockId := disk.NewBlockID(lib.PAGE_FILE_NAME, metaPageNum)

	page.SerializeMetadata(rt.metadata)
	err := rt.diskManager.Write(blockId, page)
	return err
}

func (rt *Rtreed) readMeta() (*meta.Meta, error) {
	blockId := disk.NewBlockID(lib.PAGE_FILE_NAME, metaPageNum)
	buffer, err := rt.bufferPoolManager.FetchPage(blockId)
	if err != nil {
		return nil, err
	}

	metadata := buffer.DeserializeMetadata()
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
	blockId := d.bufferPoolManager.GetNextBlockId()
	d.metadata.SetNextBlockId(blockId)
	err := d.writeMeta()
	if err != nil {
		return err
	}
	root, err := d.getNode(d.metadata.GetRoot())
	if err != nil {
		return err
	}
	_ = root

	err = d.bufferPoolManager.FlushAll()
	if err != nil {
		return err
	}
	d.bufferPoolManager.Close()
	return d.diskManager.Close()
}

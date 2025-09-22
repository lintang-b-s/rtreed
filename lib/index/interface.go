package index

import (
	"github.com/lintang-b-s/lbs/lib/buffer"
	"github.com/lintang-b-s/lbs/lib/disk"
	"github.com/lintang-b-s/lbs/lib/log"
	"github.com/lintang-b-s/lbs/lib/tree"
)

type DiskManagerI interface {
	Read(blockID disk.BlockID, page *disk.Page) error
	Write(blockID disk.BlockID, page *disk.Page) error
	Append(fileName string) (disk.BlockID, error)
	BlockLength(fileName string) (int, error)
	BlockSize() int
	IsNew() bool
	GetDBDir() string
	Close() error
}

type LogManagerI interface {
	Flush(lsn int) error
	Flush2() error
	GetIterator() (*log.LogIterator, error)
}

type BufferPoolManager interface {
	UnpinPage(blockID disk.BlockID, isDirty bool) bool
	FetchPage(blockID disk.BlockID) (*buffer.Buffer, error)
	FlushAll() error
	Close()
	NewPage(blockID *disk.BlockID) (*buffer.Buffer, error)
	GetPage(frameId int) (*tree.Node, bool)
	SetNextBlockId(nextBlockID int)
	GetNextBlockId() int
}

package index

import (
	"github.com/lintang-b-s/lbs/lib/disk"
	"github.com/lintang-b-s/lbs/lib/log"
)

type DiskManagerI interface {
	Read(blockID disk.BlockID, page *disk.Page) error
	Write(blockID disk.BlockID, page *disk.Page) error
	Append(fileName string) (disk.BlockID, error)
	BlockLength(fileName string) (int, error)
	BlockSize() int
	IsNew() bool
	GetDBDir() string
}

type LogManagerI interface {
	Flush(lsn int) error
	Flush2() error
	GetIterator() (*log.LogIterator, error)
}

type BufferPoolManager interface {
	UnpinPage(blockID disk.BlockID, isDirty bool) bool
	FetchPage(blockID disk.BlockID) (*disk.Page, error)
	FlushAll() error
	Close()
	NewPage(blockID *disk.BlockID) (*disk.Page, error)
}

package lib

var (
	MAX_BUFFER_POOL_SIZE_IN_MB = 100
	MAX_PAGE_SIZE              = 4096
	MAX_BUFFER_POOL_SIZE       = MAX_BUFFER_POOL_SIZE_IN_MB * 1024 * 1024 / MAX_PAGE_SIZE
	PAGE_SIZE_ARRAY            = []int{1024, 2048, 4096, 8192} // in bytes

)

const (
	DB_DIR         = "go_rtreed_db"
	PAGE_FILE_NAME = "go_rtreed.page"
	LOG_FILE_NAME  = "go_rtreed.log"
	NEW_PAGE_NUM   = 2 // initial new page num is 2 (0 is meta, 1 is root)
)

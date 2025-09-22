package lib

const (
	MAX_BUFFER_POOL_SIZE_IN_MB = 300
	MAX_PAGE_SIZE              = 16384
	MAX_BUFFER_POOL_SIZE       = MAX_BUFFER_POOL_SIZE_IN_MB * 1024 * 1024 / MAX_PAGE_SIZE

	DB_DIR         = "go_rtreed_db"
	PAGE_FILE_NAME = "go_rtreed.page"
	LOG_FILE_NAME  = "go_rtreed.log"
)

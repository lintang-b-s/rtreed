package lib

import "errors"

func CeilPageSize(maxPageSize int) (int, error) {
	for _, size := range PAGE_SIZE_ARRAY {
		if maxPageSize <= size {
			return size, nil
		}
	}
	return -1, errors.New("page size too large")
}

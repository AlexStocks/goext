package gxos
// refers to https://github.com/detailyang/go-fallocate1

import (
	"os"
	"syscall"
)

func Fallocate(file *os.File, offset int64, length int64) error {
	if length == 0 {
		return nil
	}

	return syscall.Fallocate(int(file.Fd()), 0, offset, length)
}

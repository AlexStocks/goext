package gxos
// refers to https://github.com/detailyang/go-fallocate1

import (
	"io/ioutil"
	"os"
	"testing"
)

func checkFileSize(f *os.File, size int64) bool {
	fs, err := f.Stat()
	if err != nil {
		return false
	}
	return fs.Size() == size
}

func fallocateWithNewFile(t *testing.T, size int64) {
	f, err := ioutil.TempFile("", "AllocateFileRange.*.txt")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		defer os.Remove(f.Name())
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()
	_ = Fallocate(f, 0, size)
	if !checkFileSize(f, size) {
		t.Errorf("Allocate file from %d to %d failed", 0, size)
	}

	_ = Fallocate(f, size, size)
	if !checkFileSize(f, 2*size) {
		t.Errorf("Allocate file from %d to %d failed", size, 2*size)
	}

	_ = Fallocate(f, 2*size-1, size)
	if !checkFileSize(f, 2*size-1+size) {
		t.Errorf("Allocate file from %d to %d failed", 2*size-1, 2*size-1+size)
	}
}

func TestFallbackFallocate(t *testing.T) {
	sizes := []int64{7, 3, 2, 1, 66666}
	for _, size := range sizes {
		fallocateWithNewFile(t, size)
	}
}

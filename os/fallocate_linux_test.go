package gxos
// refers to https://github.com/detailyang/go-fallocate1

import "testing"

func TestDarwinFallocate(t *testing.T) {
	sizes := []int64{392, 4, 1, 237, 99999}
	for _, size := range sizes {
		fallocateWithNewFile(t, size)
	}
}

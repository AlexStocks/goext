package gxos

import (
	"testing"
)

import (
	jerrors "github.com/juju/errors"
)

func TestIsSameFile(t *testing.T) {
	if !IsSameFile("./file.go", "../os/file.go") {
		t.Errorf("%s and %s is the same file", "./file.go", "../os/file.go")
	}
}

func TestGetFileModifyTime(t *testing.T) {
	tm, err := GetFileModifyTime("./file.go")
	if err != nil {
		t.Errorf("GetModifyTime(%s) = err:%s", "./file.go", jerrors.ErrorStack(err))
	}
	t.Logf("file %s modify time %s", "./file.go", tm)
}

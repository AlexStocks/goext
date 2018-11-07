package gxfilepath

import "testing"

// if @prefix is "", the files is [filepath/path.go filepath/path_test.go]
// if @prefix is "hello", the files is [hello/filepath/path.go hello/filepath/path_test.go]
func TestDirFiles(t *testing.T) {
	files, err := DirFiles("../", "hello")
	if err != nil {
		t.Errorf("err:%v", err)
	}
	t.Logf("files:%v, err:%v", files, err)
}

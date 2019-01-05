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

func TestMkdirf(t *testing.T) {
	// 创建成功
	dir := "./d/b/c"
	// if err := os.MkdirAll(dir, 0766); err != nil {
	if err := Mkdirf(dir); err != nil {
		t.Logf("create dir %s err:%s\n", dir, err)
	}
	dir = "./d/"
	if err := Rmdirf(dir); err != nil {
		t.Logf("remove dir %s err:%s\n", dir, err)
	}
}

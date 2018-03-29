package gxzlib

import "testing"

func TestDoZlibCompress(t *testing.T) {
	var str = "hello, world"
	zipStr := DoZlibCompress([]byte(str))
	uncompressString := DoZlibUncompress(zipStr)
	if string(uncompressString) != str {
		t.Errorf("str:%q, uncompress string:%q", str, string(uncompressString))
	}
}

package gxgzip

import (
	"testing"
)

func TestDoGzipCompress(t *testing.T) {
	var str = "hello, world"
	zipStr := DoGzipCompress([]byte(str))
	uncompressString := DoGzipUncompress(zipStr)
	if string(uncompressString) != str {
		t.Errorf("str:%q, uncompress string:%q", str, string(uncompressString))
	}
}

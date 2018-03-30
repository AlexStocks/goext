// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.
//
// gxgzip do gzip compress/uncompress
package gxgzip

import (
	"bytes"
	"compress/gzip"
	"io"
)

// DoGzipCompresss do gzip compress
func DoGzipCompress(src []byte) []byte {
	var in bytes.Buffer
	w := gzip.NewWriter(&in)
	w.Write(src)
	w.Close()
	return in.Bytes()
}

// DoGzipUncompresss do gzip uncompress
func DoGzipUncompress(compressSrc []byte) []byte {
	b := bytes.NewReader(compressSrc)
	var out bytes.Buffer
	r, _ := gzip.NewReader(b)
	io.Copy(&out, r)
	return out.Bytes()
}

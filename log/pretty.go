// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.

// package gxlog is based on log4go.
// pretty.go provides pretty format string
package gxlog

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/k0kubun/pp"
)

func PrettyString(i interface{}) string {
	return spew.Sdump(i)
}

func ColorPrint(i interface{}) {
	pp.Print(i)
}

func ColorPrintln(i interface{}) {
	pp.Println(i)
}

func ColorPrintf(fmt string, args ...interface{}) {
	pp.Printf(fmt, args...)
}

// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.

// package gxlog is based on log4go.
// pretty.go provides pretty format string
package gxlog

import (
	"github.com/davecgh/go-spew/spew"
)

func PrettyStruct(t interface{}) string {
	return spew.Sdump(t)
}

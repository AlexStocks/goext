// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.
//
// 2016/09/28
// Package gxstrings implements string related utilities.

package gxstrings

import (
	"unicode/utf8"
)

// get utf8 character numbers
func Stringlen(s string) int {
	return utf8.RuneCountInString(s)
}

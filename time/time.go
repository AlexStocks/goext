// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.

// Package time encapsulates some golang.time functions
package time

import (
	"time"
)

func TimeSecondDuration(sec int) time.Duration {
	return time.Duration(sec) * time.Second
}

func TimeMillisecondDuration(m int) time.Duration {
	return time.Duration(m) * time.Millisecond
}

func TimeMicrosecondDuration(m int) time.Duration {
	return time.Duration(m) * time.Microsecond
}

func TimeNanosecondDuration(n int) time.Duration {
	return time.Duration(n) * time.Nanosecond
}

// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.

// Package gxtime encapsulates some golang.time functions
package gxtime

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

// desc: convert year-month-day-hour-minute-seccond to int in second
// @month: 1 ~ 12
// @hour:  0 ~ 23
// @minute: 0 ~ 59
func YMD(year int, month int, day int, hour int, minute int, sec int) int {
	return int(time.Date(year, time.Month(month), day-1, hour-1, minute, sec, 0, time.UTC).Unix())
}

func PrintTime(sec int, nsec int) string {
	return time.Unix(int64(sec), int64(nsec)).Format("2006-01-02 15:04:05.99999")
}

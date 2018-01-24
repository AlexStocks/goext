// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxtime encapsulates some golang.time functions
package gxtime

import (
	"time"
)

type Timer struct {
	c  <-chan time.Time
	ID TimerID
	w  *TimerWheel
}

func After(d time.Duration) <-chan time.Time {
	return defaultTimerWheel.After(d)
}

func Sleep(d time.Duration) {
	defaultTimerWheel.Sleep(d)
}

func AfterFunc(d time.Duration, f func()) *Timer {
	return defaultTimerWheel.AfterFunc(d, f)
}

func NewTimer(d time.Duration) *Timer {
	return defaultTimerWheel.NewTimer(d)
}

func (t *Timer) Reset(d time.Duration) {
	if t.w == nil {
		panic("time: Stop called on uninitialized Timer")
	}

	t.w.resetTimer(t, d)
}

func (t *Timer) Stop() {
	if t.w == nil {
		panic("time: Stop called on uninitialized Timer")
	}

	t.w.deleteTimer(t)
}

// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxtime encapsulates some golang.time functions
package gxtime

import (
	"time"
)

type Timer struct {
	C  <-chan time.Time
	ID TimerID
}

func After(d time.Duration) <-chan time.Time {
	defaultTimerOnce.Do(func() {
		defaultTimer = NewTimerWheel()
	})

	timer := defaultTimer.NewTimer(d)
	if timer == nil {
		return nil
	}

	return timer.C
}

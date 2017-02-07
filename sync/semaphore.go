// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of s source code is
// governed by a BSD-style license.
package gxsync

import (
	"time"
)

type Empty struct{}

type Semaphore struct {
	lock chan Empty
}

func NewSemaphore(parallelNum int) *Semaphore {
	return &Semaphore{lock: make(chan Empty, parallelNum)}
}

func (s *Semaphore) Post() {
	<-s.lock
}

// func (s *Semaphore) Release() {
// 	var num = cap(s.lock) - len(s.lock)
// 	for i := 0; i < num; i++ {
// 		<-s.lock
// 	}
// }

func (s *Semaphore) Wait() {
	s.lock <- Empty{}
}

func (s *Semaphore) TryWait() bool {
	select {
	case s.lock <- Empty{}:
		return true
	default:
		return false
	}
}

// @timeout is in nanoseconds
func (s *Semaphore) TimeWait(timeout int) bool {
	select {
	case s.lock <- Empty{}:
		return true
	case <-time.After(time.Duration(timeout * int(time.Nanosecond))):
		return false
	}
}

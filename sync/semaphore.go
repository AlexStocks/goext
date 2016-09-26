// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.
package gxsync

import (
	"time"
)

type empty struct{}

type Semaphore struct {
	lock chan empty
}

func NewSemaphore(parallelNum int) *Semaphore {
	return &Semaphore{lock: make(chan empty, parallelNum)}
}

func (this *Semaphore) Post() {
	<-this.lock
}

// func (this *Semaphore) Release() {
// 	var num = cap(this.lock) - len(this.lock)
// 	for i := 0; i < num; i++ {
// 		<-this.lock
// 	}
// }

func (this *Semaphore) Wait() {
	this.lock <- empty{}
}

func (this *Semaphore) TryWait() bool {
	select {
	case this.lock <- empty{}:
		return true
	default:
		return false
	}
}

// @timeout is in nanoseconds
func (this *Semaphore) TimeWait(timeout int) bool {
	select {
	case this.lock <- empty{}:
		return true
	case time.After(time.Duration(timeout * time.Nanosecond)):
		return false
	}
}

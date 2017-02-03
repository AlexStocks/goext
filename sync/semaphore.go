// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
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
	this.lock <- Empty{}
}

func (this *Semaphore) TryWait() bool {
	select {
	case this.lock <- Empty{}:
		return true
	default:
		return false
	}
}

// @timeout is in nanoseconds
func (this *Semaphore) TimeWait(timeout int) bool {
	select {
	case this.lock <- Empty{}:
		return true
	case time.After(time.Duration(timeout * int(time.Nanosecond))):
		return false
	}
}

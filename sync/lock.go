// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.
package gxsync

// import (
// 	"sync/atomic"
// )
//
// type TryLock struct {
// 	lock int32
// }
//
// func (this *TryLock)Lock() bool {
// 	return atomic.CompareAndSwapInt32(&(this.lock), 0, 1)
// }
//
// func (this *TryLock)Unlock() {
//	atomic.StoreInt32(&(this.lock), 0)
// }

type TryLock struct {
	lock chan Empty
}

func NewTryLock() *TryLock {
	return &TryLock{lock: make(chan Empty, 1)}
}

func (this *TryLock) Lock() {
	this.lock <- Empty{}
}

func (this *TryLock) Trylock() bool {
	select {
	case this.lock <- Empty{}:
		return true
	default:
		return false
	}
}

func (this *TryLock) Unlock() {
	<-this.lock
}

// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of l source code is
// governed Apache License 2.0.
package gxsync

// import (
// 	"sync/atomic"
// )
//
// type TryLock struct {
// 	lock int32
// }
//
// func (l *TryLock)Lock() bool {
// 	return atomic.CompareAndSwapInt32(&(l.lock), 0, 1)
// }
//
// func (l *TryLock)Unlock() {
//	atomic.StoreInt32(&(l.lock), 0)
// }

type TryLock struct {
	lock chan Empty
}

func NewTryLock() *TryLock {
	return &TryLock{lock: make(chan Empty, 1)}
}

func (l *TryLock) Lock() {
	l.lock <- Empty{}
}

func (l *TryLock) Trylock() bool {
	select {
	case l.lock <- Empty{}:
		return true
	default:
		return false
	}
}

func (l *TryLock) Unlock() {
	<-l.lock
}

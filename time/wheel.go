// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.

// Package gxtime encapsulates some golang.time functions
package gxtime

// refer to https://github.com/siddontang/go/blob/master/timingring/timingring.go

import (
	"sync"
	"time"
)

type empty struct{}

type Wheel struct {
	sync.Mutex
	span   time.Duration
	period time.Duration
	ticker *time.Ticker
	index  int
	ring   []chan empty
	// done   chan empty
	once sync.Once
}

func NewWheel(span time.Duration, buckets int) *Wheel {
	var (
		this *Wheel
	)

	if span == 0 {
		panic("@span == 0")
	}
	if buckets == 0 {
		panic("@bucket == 0")
	}

	this = &Wheel{
		span:   span,
		period: span * (time.Duration(buckets)),
		ticker: time.NewTicker(span),
		index:  0,
		ring:   make([](chan empty), buckets),
		// done:   make(chan empty),
	}

	for idx := range this.ring {
		this.ring[idx] = make(chan empty)
	}

	go func() {
		var notify chan empty
		// for {
		// 		select {
		// 		case <-this.ticker.C:
		// 			this.Lock()

		// 			notify = this.ring[this.index]
		// 			this.ring[this.index] = make(chan empty)
		// 			this.index = (this.index + 1) % len(this.ring)

		// 			this.Unlock()

		// 			close(notify)
		// 		case <-this.done:
		// 			this.ticker.Stop()
		//			return
		//		}
		//	}
		for range this.ticker.C {
			this.Lock()

			notify = this.ring[this.index]
			this.ring[this.index] = make(chan empty)
			this.index = (this.index + 1) % len(this.ring)

			this.Unlock()

			close(notify)
		}
	}()

	return this
}

func (this *Wheel) Stop() {
	// 	select {
	// 	case <-this.done:
	// 		// this.done is a blocked channel. if it has not been closed, the default branch will be invoked.
	// 		return

	// 	default:
	// 		this.once.Do(func() { close(this.done) })
	// 	}
	this.once.Do(func() { this.ticker.Stop() })
}

func (this *Wheel) After(timeout time.Duration) <-chan empty {
	if timeout >= this.period {
		panic("@timeout over ring's life period")
	}

	this.Lock()
	c := this.ring[(this.index+int(timeout/this.span))%len(this.ring)]
	this.Unlock()

	return c
}

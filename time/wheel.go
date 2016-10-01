// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.

// Package gxtime encapsulates some golang.time functions
// ref: https://github.com/AlexStocks/go-practice/blob/master/time/siddontang_time_wheel.go
package gxtime

import (
	//"fmt"
	"sync"
	"time"
)

import (
	"github.com/AlexStocks/goext/container/bitmap"
)

type empty struct{}

type Wheel struct {
	sync.Mutex
	span   time.Duration
	period time.Duration
	ticker *time.Ticker
	index  int
	ring   []chan empty
	bitmap gxbitmap.Bitmap
	once   sync.Once
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
		bitmap: gxbitmap.NewBitmap(buckets),
	}

	for idx := range this.ring {
		this.ring[idx] = make(chan empty)
	}

	go func() {
		var notify chan empty
		// var cw CountWatch
		// cw.Start()
		for range this.ticker.C {
			this.Lock()

			// fmt.Println("index:", this.index, ", value:", this.bitmap.Get(this.index))
			if this.bitmap.Get(this.index) != 0 {
				notify = this.ring[this.index]
				this.ring[this.index] = make(chan empty) // create a new channel when its bit pos is nonzero.
				this.bitmap.Clear(this.index)
			}
			this.index = (this.index + 1) % len(this.ring)

			this.Unlock()

			if notify != nil {
				close(notify)
				notify = nil
			}
		}
		// fmt.Println("timer costs:", cw.Count()/1e9, "s")
	}()

	return this
}

func (this *Wheel) Stop() {
	this.once.Do(func() { this.ticker.Stop() })
}

func (this *Wheel) After(timeout time.Duration) <-chan empty {
	if timeout >= this.period {
		panic("@timeout over ring's life period")
	}

	var pos = int(timeout / this.span)
	if 0 < pos {
		pos--
	}

	this.Lock()
	pos = (this.index + pos) % len(this.ring)
	// fmt.Println("pos:", pos)
	c := this.ring[pos]
	this.bitmap.Set(pos)
	this.Unlock()

	return c
}

// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxtime encapsulates some golang.time functions
package gxtime

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

import (
	"github.com/AlexStocks/goext/container/xorlist"
	"github.com/AlexStocks/goext/log"
)

const (
	MAX_TIMER_LEVEL = 5
)

type TimerType int32

const (
	ETimerOnce TimerType = 0X1 << 0
	ETimerLoop TimerType = 0X1 << 1
)

var (
	defaultTimerOnce sync.Once
	defaultTimer     *TimerWheel
	nextID           TimerID
	curGxTime        int64 = time.Now().UnixNano() // current goext time in nanoseconds
)

var (
	ErrTimeChannelFull   = fmt.Errorf("timer channel full")
	ErrTimeChannelClosed = fmt.Errorf("timer channel closed")
)

////////////////////////////////////////////////
// timer node
////////////////////////////////////////////////

const (
	MAX_MS       = 1000
	MAX_SECOND   = 60
	MAX_MINUTE   = 60
	MAX_HOUR     = 24
	MAX_DAY      = 31
	MS           = 1e6
	SECOND_MS    = 1 * MAX_MS * MS
	MINUTE_MS    = 1 * MAX_SECOND * SECOND_MS
	HOUR_MS      = 1 * MAX_MINUTE * MINUTE_MS
	DAY_MS       = 1 * MAX_HOUR * HOUR_MS
	MINIMUM_DIFF = 500 * MS
)

func MS_NUM(expire int64) int64     { return expire / MS }
func SECOND_NUM(expire int64) int64 { return expire / SECOND_MS }
func MINUTE_NUM(expire int64) int64 { return expire / MINUTE_MS }
func HOUR_NUM(expire int64) int64   { return expire / HOUR_MS }
func DAY_NUM(expire int64) int64    { return expire / DAY_MS }

// if the return error is not nil, the related timer will be closed.
type TimerFunc func(ID TimerID, expire time.Time, arg interface{}) error

type TimerID = uint64

type timerNode struct {
	ID     TimerID
	trig   int64
	typ    TimerType
	period int64
	run    TimerFunc
	arg    interface{}
}

func newTimerNode(f TimerFunc, typ TimerType, period int64, arg interface{}) timerNode {
	return timerNode{
		ID:     atomic.AddUint64(&nextID, 1),
		trig:   atomic.LoadInt64(&curGxTime) + period,
		typ:    typ,
		period: period,
		run:    f,
		arg:    arg,
	}
}

func compareTimerNode(first, second timerNode) int {
	var ret int

	if first.trig < second.trig {
		ret = -1
	} else if first.trig > second.trig {
		ret = 1
	} else {
		ret = 0
	}

	return ret
}

////////////////////////////////////////////////
// timer wheel
////////////////////////////////////////////////

const (
	timerNodeQueueSize = 128
)

// timer based on multiple wheels
type TimerWheel struct {
	clock  int64                               // current time in nanosecond
	number int64                               // timer node number
	hand   [MAX_TIMER_LEVEL]int64              // clock
	slot   [MAX_TIMER_LEVEL]*gxxorlist.XorList // timer list

	timerQ chan timerNode

	once   sync.Once // for close ticker
	ticker *time.Ticker
	wg     sync.WaitGroup
}

func (t *TimerWheel) output() {
	for idx := range t.slot {
		gxlog.CDebug("print slot %d\n", idx)
		// t.slot[idx].Output()
	}
}

func NewTimerWheel() *TimerWheel {
	w := &TimerWheel{
		clock:  atomic.LoadInt64(&curGxTime),
		ticker: time.NewTicker(MINIMUM_DIFF / 1000000),
		timerQ: make(chan timerNode, timerNodeQueueSize),
	}

	for i := 0; i < MAX_TIMER_LEVEL; i++ {
		w.slot[i] = gxxorlist.New()
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		// var cw CountWatch
		// cw.Start()
		var (
			t     time.Time
			cFlag bool
			node  timerNode
			qFlag bool
			guard bool
		)

		for {
			select {
			case t, cFlag = <-w.ticker.C:
				if cFlag {
					atomic.StoreInt64(&curGxTime, t.UnixNano())
					if w.timerUpdate(t) == 0 {
						w.run()
					}

					break
				}

				guard = true

			case node, qFlag = <-w.timerQ:
				// gxlog.CError("new node:%#v, qFlag:%#v\n", node, qFlag)
				if qFlag {
					w.timerInsertNode(node)
					break
				}

				guard = true
			}

			if guard {
				break
			}
		}
	}()

	return w
}

func (w *TimerWheel) run() {
	var (
		clock int64
		err   error
		node  timerNode
		slot  *gxxorlist.XorList
	)

	slot = w.slot[0]
	clock = atomic.LoadInt64(&w.clock)
	for e, p := slot.Front(); e != nil; p, e = e, e.Next(p) {
		node = e.Value.(timerNode)
		if clock < node.trig {
			break
		}

		temp := e
		e, p = p, p.Prev(e)
		slot.Remove(temp)

		err = node.run(node.ID, UnixNano2Time(clock), node.arg)
		if err == nil && node.typ == ETimerLoop {
			w.timerInsertNode(node)
		} else {
			atomic.AddInt64(&w.number, -1)
		}
	}
}

func (w *TimerWheel) insertSlot(idx int, node timerNode) {
	var (
		pos  *gxxorlist.XorElement
		slot *gxxorlist.XorList
	)

	slot = w.slot[idx]
	for e, p := slot.Front(); e != nil; p, e = e, e.Next(p) {
		if compareTimerNode(node, e.Value.(timerNode)) < 0 {
			pos = e
			break
		}
	}

	// gxlog.CInfo("before insert, level %d slot:", idx)
	if pos != nil {
		// gxlog.CInfo("insertSlot.InsertBefore(pos:%d, node:%#v)", idx, node.ID)
		slot.InsertBefore(node, pos)
	} else {
		// if slot is empty or @node_ptr is the maximum node
		// in slot, insert it at the last of slot
		// gxlog.CInfo("insertSlot.PushBack(node:%#v)", idx, node.ID)
		slot.PushBack(node)
	}
	// gxlog.CInfo("after insert, level %d slot:", idx)
}

func (w *TimerWheel) timerInsertNode(node timerNode) {
	var (
		idx  int
		diff int64
	)

	diff = node.trig - atomic.LoadInt64(&w.clock)
	switch {
	case diff <= 0:
		idx = 0
	case DAY_NUM(diff) != 0:
		idx = 4
	case HOUR_NUM(diff) != 0:
		idx = 3
	case MINUTE_NUM(diff) != 0:
		idx = 2
	case SECOND_NUM(diff) != 0:
		idx = 1
	default:
		idx = 0
	}

	// gxlog.CInfo("insertSlot(idx:%d, node:%d)", idx, node.ID)
	w.insertSlot(idx, node)
}

func (w *TimerWheel) timerCascade(level int) {
	var (
		guard bool
		clock int64
		diff  int64
		cur   timerNode
	)

	clock = atomic.LoadInt64(&w.clock)
	// w.slot[level].Output()
	for e, p := w.slot[level].Front(); e != nil; p, e = e, e.Next(p) {
		cur = e.Value.(timerNode)
		diff = clock - cur.trig
		if diff <= 0 {
			guard = false
		} else {
			switch level {
			case 1:
				guard = SECOND_NUM(diff) > 0
			case 2:
				guard = MINUTE_NUM(diff) > 0
			case 3:
				guard = HOUR_NUM(diff) > 0
			case 4:
				guard = DAY_NUM(diff) > 0
			}

			if guard {
				break
			}

			w.timerInsertNode(cur)
			temp := e
			e, p = p, p.Prev(e)
			w.slot[level].Remove(temp)
		}
	}
}

var (
	limit   = [MAX_TIMER_LEVEL + 1]int64{MAX_MS, MAX_SECOND, MAX_MINUTE, MAX_HOUR, MAX_DAY}
	msLimit = [MAX_TIMER_LEVEL + 1]int64{MS, SECOND_MS, MINUTE_MS, HOUR_MS, DAY_MS}
)

func (w *TimerWheel) timerUpdate(curTime time.Time) int {
	var (
		now    int64
		idx    int32
		diff   int64
		maxIdx int32
		inc    [MAX_TIMER_LEVEL + 1]int64
	)

	now = curTime.UnixNano()
	diff = now - atomic.LoadInt64(&w.clock)
	if diff < MINIMUM_DIFF {
		return -1
	}
	atomic.StoreInt64(&w.clock, now)

	for idx = MAX_TIMER_LEVEL - 1; 0 <= idx; idx-- {
		inc[idx] = diff / msLimit[idx]
		diff %= msLimit[idx]
	}

	maxIdx = 0
	for idx = 0; idx < MAX_TIMER_LEVEL; idx++ {
		if 0 != inc[idx] {
			w.hand[idx] += inc[idx]
			inc[idx+1] += w.hand[idx] / limit[idx]
			w.hand[idx] %= limit[idx]
			maxIdx = idx + 1
		}
	}

	for idx = 1; idx < maxIdx; idx++ {
		w.timerCascade(int(idx))
	}

	return 0
}

func (w *TimerWheel) Stop() {
	w.once.Do(func() {
		close(w.timerQ)
		w.ticker.Stop()
		w.timerQ = nil
	})
}

func (w *TimerWheel) Close() {
	w.Stop()
	w.wg.Wait()
}

// add
func (w *TimerWheel) AddTimer(f TimerFunc, typ TimerType, period int64, arg interface{}) (TimerID, error) {
	if w.timerQ == nil {
		return 0, ErrTimeChannelClosed
	}

	node := newTimerNode(f, typ, period, arg)
	select {
	case w.timerQ <- node:
		atomic.AddInt64(&(w.number), 1)
		return node.ID, nil
	case <-time.After(1e9):
	}

	return 0, ErrTimeChannelFull
}

func sendTime(ID TimerID, t time.Time, arg interface{}) error {
	select {
	case arg.(chan time.Time) <- t:
	default:
	}

	return nil
}

func (w *TimerWheel) NewTimer(d time.Duration) *Timer {
	c := make(chan time.Time, 1)
	t := &Timer{
		C: c,
	}

	ID, err := w.AddTimer(sendTime, ETimerOnce, int64(d), c)
	if err == nil {
		t.ID = ID
		return t
	}

	close(c)
	return nil
}

func (w *TimerWheel) After(d time.Duration) <-chan time.Time {
	return w.NewTimer(d).C
}

// Copyright 2016 ~ 2017 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxruntime encapsulates some runtime functions
// goroutine pool
// ref: https://github.com/pingcap/tidb/blob/1592c7bc2873346565ab15ecfbc22749a775e014/util/goroutine_pool/gp.go
package gxruntime

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

import (
	"github.com/AlexStocks/goext/sync"
)

const (
	OnceLoopGoroutineNum = 30
)

const (
	PoolInit = iota
	PoolRunning
	PoolGcing
	PoolClosed
)

var (
	ErrGoroutinePoolClosed = fmt.Errorf("GoroutinePool has been closed.")
)

// Pool is a struct to represent goroutine pool.
type Pool struct {
	head        goroutine
	tail        *goroutine
	count       int // idle list size
	idleTimeout time.Duration
	sync.Mutex

	grID  int32
	state int32
	wg    sync.WaitGroup
	done  chan gxsync.Empty
	once  sync.Once
}

// goroutine is actually a background goroutine, with a channel bound for communication.
type goroutine struct {
	id      int32
	ch      chan func()
	lastRun time.Time
	pool    *Pool
	next    *goroutine
}

// New returns a new *Pool object.
func NewGoroutinePool(idleTimeout time.Duration) *Pool {
	pool := &Pool{
		idleTimeout: idleTimeout,
		state:       PoolInit,
		done:        make(chan gxsync.Empty),
	}
	pool.tail = &pool.head
	// pool.wg.Add(1)
	go pool.gc()
	return pool
}

// check whether the pool has been closed.
func (p *Pool) IsClosed() bool {
	select {
	case <-p.done:
		return true

	default:
		return false
	}
}

func (p *Pool) stop() {
	select {
	case <-p.done:
		return

	default:
		p.once.Do(func() {
			close(p.done)
		})
	}
}

func (p *Pool) Close() {
	p.stop()
	if atomic.LoadInt32(&p.state) == PoolGcing {
		atomic.CompareAndSwapInt32(&p.state, PoolGcing, PoolClosed)
	}
	p.wg.Wait()
}

// Go works like go func(), but goroutines are pooled for reusing.
// This strategy can avoid runtime.morestack, because pooled goroutine is already enlarged.
func (p *Pool) Go(f func()) error {
	if p.IsClosed() {
		return ErrGoroutinePoolClosed
	}

	g := p.get()
	g.ch <- f
	// When the goroutine finish f(), it will be put back to pool automatically,
	// so it doesn't need to call pool.put() here.

	return nil
}

func (p *Pool) get() *goroutine {
	p.Lock()
	head := &p.head
	if head.next == nil {
		p.Unlock()
		return p.alloc()
	}

	ret := head.next
	head.next = ret.next
	if ret == p.tail {
		p.tail = head
	}
	p.count--
	p.Unlock()
	ret.next = nil
	return ret
}

func (p *Pool) put(g *goroutine) {
	g.next = nil
	p.Lock()
	p.tail.next = g
	p.tail = g
	p.count++
	p.Unlock()

	if atomic.LoadInt32(&p.state) == PoolRunning {
		atomic.CompareAndSwapInt32(&p.state, PoolRunning, PoolGcing)
	}
}

func (p *Pool) alloc() *goroutine {
	id := atomic.AddInt32(&p.grID, 1)
	g := &goroutine{
		ch:   make(chan func()),
		pool: p,
		id:   id,
	}

	go func(g *goroutine) {
		for work := range g.ch {
			func() {
				g.pool.wg.Add(1)
				// do not cost much time
				defer g.pool.wg.Done()
				work()
			}()
			g.lastRun = time.Now()
			// Put g back to the pool.
			// This is the normal usage for a resource pool:
			//
			//     obj := pool.get()
			//     use(obj)
			//     pool.put(obj)
			//
			// But when goroutine is used as a resource, we can't pool.put() immediately,
			// because the resource(goroutine) maybe still in use.
			// So, put back resource is done here,  when the goroutine finish its work.
			p.put(g)
		}
	}(g)

	if atomic.LoadInt32(&p.state) == PoolInit {
		atomic.CompareAndSwapInt32(&p.state, PoolInit, PoolRunning)
	}

	return g
}

func (p *Pool) gc() {
	// defer p.wg.Done()
	var (
		finish bool
		more   bool
		state  int32
		t      time.Duration
	)
	for {
		finish = false
		more = false
		if state = atomic.LoadInt32(&p.state); state > PoolRunning {
			finish, more = p.gcOnce(OnceLoopGoroutineNum)
			if finish {
				return
			}
		}

		if atomic.LoadInt32(&p.state) == PoolClosed {
			t = min(p.idleTimeout/10, 50*time.Millisecond)
		} else if more {
			t = min(p.idleTimeout/10, 500*time.Millisecond)
		} else {
			t = min(5*time.Second, p.idleTimeout/3)
		}
		time.Sleep(t)
	}
}

// gcOnce runs gc once, recycles at most count goroutines.
// finish indicates there're no more goroutines in the pool after gc,
// more indicates there're still many goroutines to be recycled.
func (p *Pool) gcOnce(count int) (finish bool, more bool) {
	now := time.Now()
	i := 0
	p.Lock()
	head := &p.head

	for head.next != nil && i < count {
		save := head.next
		duration := now.Sub(save.lastRun)
		if duration < p.idleTimeout {
			break
		}
		close(save.ch)
		head.next = save.next
		p.count--
		i++
	}

	if head.next == nil {
		finish = true
		p.tail = head
	}

	p.Unlock()
	more = (i == count)
	return
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}

	return b
}

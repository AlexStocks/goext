// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of s source code is
// governed Apache License 2.0.
//
// this file provides a kind of unbouned channel
package gxsync

import (
	"sync"
)

import (
	"github.com/AlexStocks/goext/container/queue"
)

const (
	QSize = 64
)

// refer from redisgo/redis/pool.go
type UnboundedChan struct {
	// 如果wait为true且pool中已经分配出去的conn数目已经超过MaxActive，则get函数会等待，
	// 一直到有空闲连接为止，等待过程使用的变量就是cond; wait为false则直接返回连接池已满error
	wait   bool
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	Q      *gxqueue.Queue
}

func NewUnboundedChan() *UnboundedChan {
	c := &UnboundedChan{
		Q: gxqueue.NewQueueWithSize(QSize),
	}
	c.cond = sync.NewCond(&c.mu)

	return c
}

// 在pop时，如果没有资源，是否等待
// 即使用乐观锁还是悲观锁
func (q *UnboundedChan) SetWaitOption(wait bool) {
	q.mu.Lock()
	q.wait = wait
	q.mu.Unlock()
}

func (q *UnboundedChan) Pop() interface{} {
	var v interface{}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.Q.Length() == 0 && !q.closed && !q.wait {
		return v
	}
	for q.Q.Length() == 0 && !q.closed {
		q.cond.Wait()
	}

	if q.Q.Length() > 0 {
		v = q.Q.Peek()
		q.Q.Remove()
	}

	return v
}

func (q *UnboundedChan) TryPop() (interface{}, bool) {
	var (
		ok bool
		v  interface{}
	)

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		ok = true
	} else if q.Q.Length() > 0 {
		v = q.Q.Peek()
		q.Q.Remove()
		ok = true
	}

	return v, ok
}

func (q *UnboundedChan) Push(v interface{}) {
	q.mu.Lock()
	if !q.closed {
		q.Q.Add(v)
		q.cond.Signal()
	}
	q.mu.Unlock()
}

func (q *UnboundedChan) Len() int {
	q.mu.Lock()
	l := q.Q.Length()
	q.mu.Unlock()

	return l
}

func (q *UnboundedChan) Close() {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		q.cond.Broadcast()
	}
	q.mu.Unlock()
}
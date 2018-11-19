// Copyright 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed Apache License 2.0.

// Package pool implements a pool of Object interfaces to manage and reuse them.
package gxpool

import (
	"container/heap"
	"sync"
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
	jerrors "github.com/juju/errors"
)

type DoTask func(workerID int, request interface{}) interface{}

type Task struct {
	do   DoTask
	req  interface{}
	rspQ chan interface{} // result queue
}

type Worker struct {
	ID      int
	taskQ   chan *Task
	done    chan struct{}
	pending int // count of pending tasks
	fin     int // count of done tasks
	index   int // The index of the item in the heap.
	wg      sync.WaitGroup
	once    sync.Once
}

func NewWorker(id int, k *Keeper) *Worker {
	w := &Worker{
		ID:    id,
		taskQ: make(chan *Task, 64),
		done:  make(chan struct{}),
	}

	w.wg.Add(1)
	go w.work(k)

	return w
}

func (w *Worker) work(k *Keeper) {
	defer w.wg.Done()
	for {
		select {
		case t, ok := <-w.taskQ: // get task from balancer
			if ok {
				t.rspQ <- t.do(w.ID, t.req) // call fn and send result
				k.workerQ <- w              // we've finished w request
			} else {
				log.Warn("worker %d done channel closed, so it exits now with {its taskQ len = %d, pending = %d}",
					w.ID, len(w.taskQ), w.pending)
				return
			}
		case <-w.done:
			log.Warn("worker %d done channel closed, so it exits now with {its taskQ len = %d, pending = %d}",
				w.ID, len(w.taskQ), w.pending)
			return
		}
	}
}

func (w *Worker) stop(k *Keeper) {
	select {
	case <-w.done:
		return
	default:
		w.once.Do(func() {
			close(w.done)
		})
	}
}

type WorkerPool []*Worker

func (p WorkerPool) Less(i, j int) bool {
	return p[i].pending < p[j].pending
}

func (p WorkerPool) Len() int {
	return len(p)
}

func (p WorkerPool) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].index = i
	p[j].index = j
}

func (p *WorkerPool) Push(w interface{}) {
	n := len(*p)
	item := w.(*Worker)
	item.index = n
	*p = append(*p, item)
}

func (p *WorkerPool) Pop() interface{} {
	arr := *p
	n := len(arr)
	item := arr[n-1]
	item.index = -1 // for safety
	*p = arr[0 : n-1]

	return item
}

type Keeper struct {
	workers   WorkerPool
	workerNum int
	workerQ   chan *Worker
	taskQ     chan *Task
	wg        sync.WaitGroup
	done      chan struct{}
	once      sync.Once
}

func NewKeeper(workerNum int) *Keeper {
	k := &Keeper{
		workers:   make(WorkerPool, 0, 32),
		workerNum: workerNum,
		workerQ:   make(chan *Worker, 128),
		taskQ:     make(chan *Task, 1024),
		done:      make(chan struct{}),
	}

	heap.Init(&k.workers)
	for i := 0; i < workerNum; i++ {
		heap.Push(&k.workers, NewWorker(i, k))
	}

	k.wg.Add(1)
	go k.run()

	return k
}

func (k *Keeper) run() {
	defer k.wg.Done()
	for {
		select {
		case t := <-k.taskQ:
			if l := k.workers.Len(); l > 0 {
				worker := heap.Pop(&k.workers).(*Worker)
				worker.taskQ <- t
				worker.pending++
				heap.Push(&k.workers, worker)
			} else {
				err := TaskErrorCode(TC_ServerBusy)
				t.rspQ <- TaskError{Code: err, Desc: err.String()}
			}

		case worker := <-k.workerQ:
			worker.pending--
			worker.fin++
			heap.Remove(&k.workers, worker.index)
			heap.Push(&k.workers, worker)

		case <-k.done:
			log.Warn("keeper exit now while its task queue size = %d.", len(k.taskQ))
			return
		}
	}
}

func (k *Keeper) PushTask(t *Task, timeout time.Duration) error {
	select {
	case k.taskQ <- t:
		return nil
	case <-time.After(timeout):
		return jerrors.New(TC_WaitTimeout.String())
	}
}

func (k *Keeper) Stop() {
	select {
	case <-k.done:
		return
	default:
		k.once.Do(func() {
			close(k.done)
		})
	}
}

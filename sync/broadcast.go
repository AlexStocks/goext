// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.

package gxsync

import (
	"fmt"
)

var (
	ErrBroadcastClosed = fmt.Errorf("broadcast closed!")
)

type broadcast struct {
	c chan broadcast // 指向下一个broadcast node
	v interface{}
}

type Broadcaster struct {
	// private fields:
	Listenc chan chan (chan broadcast)
	Sendc   chan<- interface{}
}

type Receiver struct {
	// private fields:
	C chan broadcast
}

// create a new broadcaster object.
func NewBroadcaster() Broadcaster {
	listenc := make(chan (chan (chan broadcast)))
	sendc := make(chan interface{})
	go func() {
		currc := make(chan broadcast, 1)
		for {
			select {
			case v := <-sendc:
				if v == nil {
					currc <- broadcast{}
					return
				}
				c := make(chan broadcast, 1)
				b := broadcast{c: c, v: v}
				currc <- b
				currc = c
			case r := <-listenc:
				r <- currc
			}
		}
	}()
	return Broadcaster{
		Listenc: listenc,
		Sendc:   sendc,
	}
}

// start listening to the broadcasts.
func (b Broadcaster) Listen() Receiver {
	c := make(chan chan broadcast, 0)
	b.Listenc <- c
	return Receiver{<-c}
}

// broadcast a value to all listeners.
func (b Broadcaster) Write(v interface{}) (rerr error) {
	defer func() {
		if e := recover(); e != nil {
			rerr = fmt.Errorf("panic error:%v", e)
		}
	}()

	if v == nil {
		close(b.Sendc) // close之后，<- sendc返回(nil, false)
		return
	}
	b.Sendc <- v

	return
}

func (b Broadcaster) Close() { b.Write(nil) }

// read a value that has been broadcast,
// waiting until one is available if necessary.
func (r *Receiver) Read() (interface{}, error) {
	b := <-r.C
	v := b.v
	r.C <- b
	r.C = b.c // 指向链表的下一个节点

	if v == nil {
		return nil, ErrBroadcastClosed
	}

	return v, nil
}

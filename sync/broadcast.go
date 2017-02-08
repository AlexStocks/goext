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

type node struct {
	next  chan node
	value interface{}
}

type Broadcaster struct {
	registry chan chan (chan node)
	data     chan<- interface{}
}

type Receiver struct {
	R chan node
}

// create a new broadcaster object.
func NewBroadcaster() Broadcaster {
	registry := make(chan (chan (chan node)))
	data := make(chan interface{})
	go func() {
		var (
			value    interface{}
			cursor   chan node
			tmp_node chan node
			reader   chan chan node
		)
		cursor = make(chan node, 1)
		// gxlog.CError("first cursor:%#value\n", cursor)
		for {
			select {
			case value = <-data:
				if value == nil {
					cursor <- node{}
					// gxlog.CError("last cursor:%#value\n", cursor)
					return
				}
				tmp_node = make(chan node, 1)
				// b := node{next: node, value: value}
				// gxlog.CError("value:%#value, b:%#value, node:%#value, cursor:%#value\n", value, b, node, cursor)
				// cursor <- b
				cursor <- node{next: tmp_node, value: value}
				cursor = tmp_node
				// gxlog.CError("cursor:%#value\n", cursor)
			case reader = <-registry:
				reader <- cursor
				// gxlog.CError("new reader:%#value", reader)
			}
		}
	}()
	return Broadcaster{
		registry: registry,
		data:     data,
	}
}

// start listening to the nodes.
func (b Broadcaster) Listen() Receiver {
	r := make(chan chan node, 0)
	// gxlog.CError("new reader channel:%#value", node)
	b.registry <- r
	return Receiver{<-r}
}

// node a value to all listeners.
func (b Broadcaster) Write(value interface{}) (rerr error) {
	defer func() {
		if e := recover(); e != nil {
			rerr = fmt.Errorf("panic error:%value", e)
		}
	}()

	if value == nil {
		close(b.data)
		return
	}
	b.data <- value

	return
}

func (b Broadcaster) Close() { b.Write(nil) }

// read a value that has been broadcast,
// waiting until one is available if necessary.
func (r *Receiver) Read() (interface{}, error) {
	b := <-r.R
	value := b.value
	r.R <- b
	// gxlog.CDebug("b:%#value, r:%#value", b, r)
	r.R = b.next
	// gxlog.CDebug("new r:%#value", r)

	if value == nil {
		return nil, ErrBroadcastClosed
	}

	return value, nil
}

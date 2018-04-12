// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxregistry provides a interface for service register/discovery
package gxregistry

import (
	"fmt"
)

import (
	jerrors "github.com/juju/errors"
)

//////////////////////////////////////////
// service url event type
//////////////////////////////////////////

type ServiceEventType int

const (
	ServiceAdd = iota
	ServiceDel
	ServiceUpdate
)

var serviceURLEventTypeStrings = [...]string{
	"add service",
	"delete service",
	"update service",
}

func (t ServiceEventType) String() string {
	return serviceURLEventTypeStrings[t]
}

// Result is returned by a call to Next on
// the watcher. Actions can be create, update, delete
type Result struct {
	Action  ServiceEventType
	Service *Service
}

func (r Result) String() string {
	return fmt.Sprintf("Result{Action{%s}, Service{%#v}}", r.Action, r.Service)
}

// Watcher is an interface that returns updates
// about services within the registry.
type Watcher interface {
	// Next is a blocking call
	Next() (*Result, error)
	Valid() bool // 检查watcher与registry连接是否正常
	Stop()
	IsClosed() bool
}

var (
	ErrWatcherClosed = jerrors.Errorf("Watcher closed")
)

// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxregistry provides a interface for service register/discovery
package gxregistry

import (
	jerrors "github.com/juju/errors"
)

// Watcher is an interface that returns updates
// about services within the registry.
type Watcher interface {
	// Next is a blocking call
	Next() (*EventResult, error)
	Valid() bool // 检查watcher与registry连接是否正常
	Stop()
	IsClosed() bool
}

var (
	ErrWatcherClosed = jerrors.Errorf("Watcher closed")
)

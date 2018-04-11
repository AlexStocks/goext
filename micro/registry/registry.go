// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxregistry provides a interface for service register/discovery
package gxregistry

import (
	jerrors "github.com/juju/errors"
)

// The registry provides an interface for service discovery
// and an abstraction over varying implementations
// {consul, etcd, zookeeper, ...}
type Registry interface {
	// Register(conf ServiceConfig) error
	Register(conf interface{}) error
	GetService(string) ([]*Service, error)
	ListServices() ([]*Service, error)
	Watch() (Watcher, error)
	Close()
	String() string
}

const (
	REGISTRY_CONN_DELAY = 3 // watchDir中使用，防止不断地对zk重连
)

var (
	ErrorRegistryNotFound = jerrors.NewErr("registry not found")
)

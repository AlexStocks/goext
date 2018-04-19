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
// {etcd, zookeeper, ...}
type Registry interface {
	Register(service Service) error
	Unregister(service Service) error
	GetService(attr ServiceAttr) (*Service, error)
	Watch(opts ...WatchOption) Watcher
	Close() error
	String() string
	Options() Options
}

const (
	REGISTRY_CONN_DELAY = 3 // watchDir中使用，防止不断地对zk重连
	DefaultTimeout      = 3e9
	MaxFailTime         = 15e9 // fail retry wait time delay
)

var (
	ErrorRegistryNotFound = jerrors.Errorf("registry not found")
	ErrorAlreadyRegister  = jerrors.Errorf("service has already been registered")
	DefaultServiceRoot    = "/gxregistry"
)

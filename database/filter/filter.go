// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxfilter provides a interface for service filter
package gxfilter

import (
	"github.com/AlexStocks/goext/database/registry"
	jerrors "github.com/juju/errors"
)

type ServiceToken = int64

// Balancer returns a available node based on its load balance algorithm.
type Balancer func(ID uint64) (*gxregistry.Service, error)

// Filter used to get service nodes from registry.
type Filter interface {
	Options() Options
	GetService(attr gxregistry.ServiceAttr) ([]*gxregistry.Service, error)
	Filter(attr gxregistry.ServiceAttr) (Balancer, ServiceToken, error)
	CheckTokenAlive(attr gxregistry.ServiceAttr, token ServiceToken) bool
	Close() error
}

var (
	ErrNotFound              = jerrors.Errorf("not found")
	ErrNoneAvailable         = jerrors.Errorf("none available")
	ErrRunOutAllServiceNodes = jerrors.Errorf("has used out all provider nodes")
)

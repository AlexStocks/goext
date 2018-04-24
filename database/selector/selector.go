// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxselector provides a interface for service selector
package gxselector

import (
	"github.com/AlexStocks/goext/database/registry"
	jerrors "github.com/juju/errors"
)

// Filter returns a available node based on its load balance algorithm.
type Filter func(ID uint64) (*gxregistry.Service, error)

// Selector used to get service nodes from registry.
type Selector interface {
	Init(opts ...Option) error
	Options() Options
	Select(attr gxregistry.ServiceAttr) (Filter, error)
	Close() error
	String() string
}

var (
	ErrNotFound              = jerrors.Errorf("not found")
	ErrNoneAvailable         = jerrors.Errorf("none available")
	ErrRunOutAllServiceNodes = jerrors.Errorf("has used out all provider nodes")
)

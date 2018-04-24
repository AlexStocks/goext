// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxselector provides a interface for service selector
package gxselector

import (
	"github.com/AlexStocks/goext/database/registry"
	jerrors "github.com/juju/errors"
)

type ServiceToken = int64

// Filter returns a available node based on its load balance algorithm.
type Filter func(ID uint64) (*gxregistry.Service, error)

// Selector used to get service nodes from registry.
type Selector interface {
	Options() Options
	Select(attr gxregistry.ServiceAttr) (Filter, ServiceToken, error)
	CheckTokenAlive(attr gxregistry.ServiceAttr, token ServiceToken) bool
	Close() error
}

var (
	ErrNotFound              = jerrors.Errorf("not found")
	ErrNoneAvailable         = jerrors.Errorf("none available")
	ErrRunOutAllServiceNodes = jerrors.Errorf("has used out all provider nodes")
)

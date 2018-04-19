// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxregistry provides a interface for service selector
package gxselector

import (
	"github.com/AlexStocks/goext/database/registry"
	jerrors "github.com/juju/errors"
)

// Selector builds on the registry as a mechanism to pick nodes
// and mark their status. This allows host pools and other things
// to be built using various algorithms.
type Selector interface {
	Init(opts ...Option) error
	Options() Options
	// Select returns a function which should return the next node
	Select(attr gxregistry.ServiceAttr) (Next, error)
	// Close renders the selector unusable
	Close() error
	// Name of the selector
	String() string
}

// Next is a function that returns the next node
// based on the selector's strategy
type Next func(ID uint64) (*gxregistry.Service, error)

var (
	ErrNotFound              = jerrors.Errorf("not found")
	ErrNoneAvailable         = jerrors.Errorf("none available")
	ErrRunOutAllServiceNodes = jerrors.Errorf("has used out all provider nodes")
)

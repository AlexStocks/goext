// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxregistry provides a interface for service selector
package gxselector

import (
	"context"
)

import (
	"github.com/AlexStocks/goext/database/registry"
)

type Options struct {
	Registry gxregistry.Registry
	Mode     SelectorMode // selector mode

	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

// Option used to initialise the selector
type Option func(*Options)

// WithRegistry sets the registry used by the selector
func WithRegistry(r gxregistry.Registry) Option {
	return func(o *Options) {
		o.Registry = r
	}
}

// WithSelectMode sets the default strategy for the selector
func WithSelectMode(mode SelectorMode) Option {
	return func(o *Options) {
		o.Mode = mode
	}
}

func WithContext(ctx context.Context) Option {
	return func(o *Options) {
		o.Context = ctx
	}
}

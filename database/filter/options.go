// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxfilter provides a interface for service filter
package gxfilter

import (
	"github.com/AlexStocks/goext/context"
	"github.com/AlexStocks/goext/database/registry"
)

type Options struct {
	Registry gxregistry.Registry
	Mode     BalancerMode // filter mode
	Context  *gxcontext.ValuesContext
}

// Option used to initialise the filter
type Option func(*Options)

// WithRegistry sets the registry used by the filter
func WithRegistry(r gxregistry.Registry) Option {
	return func(o *Options) {
		o.Registry = r
	}
}

// WithBalancerMode sets the default strategy for the filter
func WithBalancerMode(mode BalancerMode) Option {
	return func(o *Options) {
		o.Mode = mode
	}
}

func WithContext(ctx *gxcontext.ValuesContext) Option {
	return func(o *Options) {
		o.Context = ctx
	}
}

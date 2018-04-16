// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxregistry provides a interface for service register/discovery
package gxregistry

import (
	"context"
	"time"
)

type Options struct {
	Addrs   []string
	Timeout time.Duration
	Root    string

	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

type RegisterOptions struct {
	TTL time.Duration
	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

type WatchOptions struct {
	// the root registry path, suck as "/dubbo/"
	Root string
	// Specify a service to watch
	// If blank, the watch is for all services
	Service string
	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

type Option func(*Options)

type RegisterOption func(*RegisterOptions)

// Addrs is the registry addresses to use
func WithAddrs(addrs ...string) Option {
	return func(o *Options) {
		o.Addrs = addrs
	}
}

func WithTimeout(t time.Duration) Option {
	return func(o *Options) {
		o.Timeout = t
	}
}

func WithRoot(root string) Option {
	return func(o *Options) {
		o.Root = root
	}
}

func WithRegisterTTL(t time.Duration) RegisterOption {
	return func(o *RegisterOptions) {
		o.TTL = t
	}
}

type WatchOption func(*WatchOptions)

// Watch root
func WithWatchRoot(root string) WatchOption {
	return func(o *WatchOptions) {
		o.Root = root
	}
}

// Watch a service
func WithWatchService(name string) WatchOption {
	return func(o *WatchOptions) {
		o.Service = name
	}
}

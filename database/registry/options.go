// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxregistry provides a interface for service register/discovery
package gxregistry

import (
	"time"
)

type Options struct {
	Addrs   []string
	Timeout time.Duration
	Root    string
}

type WatchOptions struct {
	// the root registry path, suck as "/dubbo/"
	Root string
}

type Option func(*Options)

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

type WatchOption func(*WatchOptions)

// Watch root
func WithWatchRoot(root string) WatchOption {
	return func(o *WatchOptions) {
		o.Root = root
	}
}

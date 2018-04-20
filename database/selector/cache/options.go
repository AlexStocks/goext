// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxregistry provides a interface for service selector
package gxcache

import (
	"context"
	"time"
)

import (
	"github.com/AlexStocks/goext/database/selector"
)

const (
	GxselectorDefaultKey = 0X201804201515
)

func WithTTL(t time.Duration) gxselector.Option {
	return func(o *gxselector.Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, GxselectorDefaultKey, t)
	}
}

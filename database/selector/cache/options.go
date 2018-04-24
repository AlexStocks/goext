// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxregistry provides a interface for service selector
package gxcache

import (
	"time"
)

import (
	"github.com/AlexStocks/goext/context"
	"github.com/AlexStocks/goext/database/selector"
)

const (
	GxselectorDefaultKey = 0X201804201515
)

func WithTTL(t time.Duration) gxselector.Option {
	return func(o *gxselector.Options) {
		if o.Context == nil {
			o.Context = gxcontext.NewValuesContext(nil)
		}
		o.Context.Set(GxselectorDefaultKey, t)
	}
}

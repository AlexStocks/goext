// Copyright 2016 ~ 2017 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// 2017-11-05 16:38
// Package gxpprof provides go process info
package gxpprof

import (
	"github.com/google/gops/agent"
	"github.com/pkg/errors"
)

func Gops() error {
	if err := agent.Listen(agent.Options{}); err != nil {
		return errors.Wrapf(err, "gops/agent.Listen()")
	}

	return nil
}

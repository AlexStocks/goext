// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// 2017-11-05 16:38
// Package gxpprof provides go process info
package gxpprof

import (
	"os"
	"os/user"

	"github.com/google/gops/agent"
	"github.com/juju/errors"
)

func guessUnixHomeDir() string {
	usr, err := user.Current()
	if err == nil {
		return usr.HomeDir
	}

	return os.Getenv("HOME")
}

// @addr: ":10001"
func Gops(addr string) error {
	opts := agent.Options{
		Addr:      addr,
		ConfigDir: guessUnixHomeDir(),
	}
	err := agent.Listen(opts)
	if err != nil {
		return errors.Annotatef(err, "gops/agent.Listen(opts:%#v)", opts)
	}

	return nil
}

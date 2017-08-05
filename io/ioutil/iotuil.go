// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed Apache License 2.0.

// Package gxioutil implements some I/O utility functions.
package gxioutil

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
)

func ReadTxt(file string) ([]byte, error) {
	if len(file) == 0 {
		return nil, errors.New("@file is nil")
	}

	if _, err := os.Stat(file); os.IsNotExist(err) {
		return nil, fmt.Errorf("@file{%s} not found", file)
	}
	f, err := os.OpenFile(file, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	data, err := ioutil.ReadAll(reader)
	if len(data) == 0 {
		return nil, fmt.Errorf("can not get content of @file{%s}", file)
	}

	return data, err
}

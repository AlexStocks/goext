// Copyright 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxxor implements the xor crypto alg.
package gxxor

import "strconv"

type Xor struct {
	key []byte
}

func NewXor(key []byte) *Xor {
	k := make([]byte, len(key), len(key))
	copy(k, key)
	return &Xor{key: k}
}

func (x *Xor) Encrypt(txt string) string {
	var (
		i, j     int
		s        string
		cryptTxt string
	)

	uTxt := []rune(txt)
	for i = 0; i < len(uTxt); i++ {
		s = strconv.FormatInt(int64(byte(uTxt[i])^x.key[j]), 16)
		if len(s) == 1 {
			s = "0" + s
		}
		cryptTxt += s
		j = (j + 1) % len(x.key)
	}

	return cryptTxt
}

func (x *Xor) Decrypt(cryptTxt string) (string, error) {
	var (
		i, j int
		s    int64
		txt  string
		err  error
	)

	uCryptTxt := []rune(cryptTxt)
	for i = 0; i < len(cryptTxt)/2; i++ {
		s, err = strconv.ParseInt(string(uCryptTxt[i*2:i*2+2]), 16, 0)
		if err != nil {
			return "", err
		}
		txt += string(byte(s) ^ x.key[j])
		j = (j + 1) % len(x.key)
	}

	return txt, nil
}

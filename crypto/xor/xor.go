// Copyright 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxxor implements the xor crypto alg.
package gxxor

import (
	"encoding/base64"
	"strconv"
)

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

func (x *Xor) EncryptBytesInBase64(txt []byte) []byte {
	byteArr := []byte(base64.StdEncoding.EncodeToString(txt))
	// fmt.Println("base64 array len:", len(byteArr))
	// fmt.Println("base64 array:", string(byteArr))
	// fmt.Printf("base64 array hex:%x\n", byteArr)
	txtLen := len(byteArr)
	keyLen := len(x.key)
	for i := 0; i < txtLen; i++ {
		byteArr[i] ^= x.key[i%keyLen]
	}
	// fmt.Println("encrypt bytes len:", len(byteArr))

	return byteArr
}

func (x *Xor) EncryptInBase64(txt string) string {
	return string(x.EncryptBytesInBase64([]byte(txt)))
}

func (x *Xor) DecryptBytesInBase64(bytes []byte) ([]byte, error) {
	keyLen := len(x.key)
	bytesLen := len(bytes)
	decBytes := make([]byte, len(bytes))
	for i := 0; i < bytesLen; i++ {
		decBytes[i] = x.key[i%keyLen] ^ bytes[i]
	}

	dbuf := make([]byte, base64.StdEncoding.DecodedLen(len(decBytes)))
	n, err := base64.StdEncoding.Decode(dbuf, decBytes)
	return dbuf[:n], err
}

func (x *Xor) DecryptInBase64(cryptTxt string) (string, error) {
	txtBytes, err := x.DecryptBytesInBase64([]byte(cryptTxt))
	return string(txtBytes), err
}

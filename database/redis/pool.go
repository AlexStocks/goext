// Copyright 2016 ~ 2017 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxredis provides a redis driver for redis.
package gxredis

import (
	"errors"
	"time"
	"net"
)

import (
	"github.com/garyburd/redigo/redis"
	"github.com/AlexStocks/go-sentinel"
)

func NewRedisPool(addr *net.TCPAddr, role RedisRole) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		MaxActive:   64,
		Wait:        true,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr.String())
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if !sentinel.TestRole(c, role.String()) {
				return errors.New("Role check failed")
			}

			_, err := c.Do("PING")
			return err
		},
	}
}
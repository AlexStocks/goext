// Copyright 2016 ~ 2017 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxredis provides a redis driver for redis.
package gxredis

// RedisRole defines the role of the redis
type RedisRole int

const (
	RR_BEGIN RedisRole = iota
	RR_Master
	RR_Slave
	RR_Sentinel
	RR_END
)

var redisRoleStrings = [...]string{
	"Begin",
	"master",
	"slave",
	"sentinel",
	"End",
}

func (r RedisRole) String() string {
	if RR_BEGIN < r && r < RR_END {
		return redisRoleStrings[r]
	}

	return ""
}

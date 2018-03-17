// Copyright 2016 ~ 2017 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxnet encapsulates some network functions
package gxnet

import (
	"net"
	"strconv"
)

// HostAddress composes a ip:port style address. Its opposite function is net.SplitHostPort.
func HostAddress(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func WSHostAddress(host string, port int, path string) string {
	return "ws://" + net.JoinHostPort(host, strconv.Itoa(port)) + path
}

func WSSHostAddress(host string, port int, path string) string {
	return "wss://" + net.JoinHostPort(host, strconv.Itoa(port)) + path
}

func HostAddress2(host string, port string) string {
	return net.JoinHostPort(host, port)
}

func WSHostAddress2(host string, port string, path string) string {
	return "ws://" + net.JoinHostPort(host, port) + path
}

func WSSHostAddress2(host string, port string, path string) string {
	return "wss://" + net.JoinHostPort(host, port) + path
}

func HostPort(addr string) (string, string, error) {
	return net.SplitHostPort(addr)
}

func IsUDPAddrEqual(addr0 *net.UDPAddr, addr1 *net.UDPAddr) bool {
	if addr0 != nil && addr1 != nil && addr0.IP.Equal(addr1.IP) && addr0.Port == addr1.Port && addr0.Zone == addr1.Zone {
		return true
	}

	return false
}

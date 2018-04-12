// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.
//
// refer: https://github.com/micro/go-micro/blob/master/registry/service.go
package gxregistry

import (
	"encoding/json"
	"strings"
)

type Service struct {
	Name      string            `json:"name"`
	Version   string            `json:"version"`
	Metadata  map[string]string `json:"metadata"`
	Endpoints []*Endpoint       `json:"endpoints"`
	Nodes     []*Node           `json:"nodes"`
}

type Node struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"`
	Port     int               `json:"port"`
	Metadata map[string]string `json:"metadata"`
}

type Endpoint struct {
	Name     string            `json:"name"`
	Request  *Value            `json:"request"`
	Response *Value            `json:"response"`
	Metadata map[string]string `json:"metadata"`
}

type Value struct {
	Name   string   `json:"name"`
	Type   string   `json:"type"`
	Values []*Value `json:"values"`
}

func EncodeService(s *Service) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func DecodeService(ds []byte) *Service {
	var s *Service
	json.Unmarshal(ds, &s)
	return s
}

func ServicePath(paths ...string) string {
	var service strings.Builder
	for _, path := range paths {
		service.WriteString(path)
		if !strings.HasSuffix(path, "/") {
			service.WriteString("/")
		}
	}

	return service.String()
}

func NodePath(nodeID string, paths ...string) string {
	return ServicePath(paths...) + nodeID
}

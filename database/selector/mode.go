// Copyright 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxselector provides a interface for service selector
package gxselector

import (
	"math/rand"
	"sync"
	"time"
)

import (
	"github.com/AlexStocks/goext/database/registry"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type SelectorModeFunc func([]*gxregistry.Service) Next

// Random is a random strategy algorithm for node selection
func random(services []*gxregistry.Service) Next {
	return func(ID uint64) (*gxregistry.Service, error) {
		if len(services) == 0 {
			return nil, ErrNoneAvailable
		}

		i := ((uint64)(rand.Int()) + ID) % (uint64)(len(services))
		return services[i], nil
	}
}

// hash is hash strategy algorithm for node selection
func hash(services []*gxregistry.Service) Next {
	return func(ID uint64) (*gxregistry.Service, error) {
		if len(services) == 0 {
			return nil, ErrNoneAvailable
		}

		i := ID % (uint64)(len(services))
		return services[i], nil
	}
}

// RoundRobin is a roundrobin strategy algorithm for node selection
func roundRobin(services []*gxregistry.Service) Next {
	var i uint64
	var mtx sync.Mutex

	return func(ID uint64) (*gxregistry.Service, error) {
		if len(services) == 0 {
			return nil, ErrNoneAvailable
		}

		mtx.Lock()
		node := services[(ID+i)%(uint64)(len(services))]
		i++
		mtx.Unlock()

		return node, nil
	}
}

//////////////////////////////////////////
// selector mode
//////////////////////////////////////////

// SelectorMode defines the algorithm of selecting a provider from cluster
type SelectorMode int

const (
	SM_BEGIN SelectorMode = iota
	SM_Random
	SM_RoundRobin
	SM_Hash
	SM_END
)

var selectorModeStrings = [...]string{
	"Begin",
	"Random",
	"RoundRobin",
	"Hash",
	"End",
}

func (s SelectorMode) String() string {
	if SM_BEGIN < s && s < SM_END {
		return selectorModeStrings[s]
	}

	return ""
}

var (
	selectorModeFuncs = []SelectorModeFunc{
		SM_BEGIN:      random,
		SM_Random:     random,
		SM_RoundRobin: roundRobin,
		SM_Hash:       hash,
		SM_END:        random,
	}
)

func SelectorNext(mode SelectorMode) SelectorModeFunc {
	if mode < SM_BEGIN || SM_END < mode {
		mode = SM_Random
	}

	return selectorModeFuncs[mode]
}

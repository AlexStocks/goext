package selector

import (
	"errors"
)

import (
	"github.com/AlexStocks/dubbogo/registry"
)

// Selector builds on the registry as a mechanism to pick nodes
// and mark their status. This allows host pools and other things
// to be built using various algorithms.
type Selector interface {
	Init(opts ...Option) error
	Options() Options
	// Select returns a function which should return the next node
	Select(service string) (Next, error)
	// Mark sets the success/error against a node
	Mark(service string, url *registry.ServiceURL, err error)
	// Reset returns state back to zero for a service
	Reset(service string)
	// Close renders the selector unusable
	Close() error
	// Name of the selector
	String() string
}

// Next is a function that returns the next node
// based on the selector's strategy
type Next func(ID uint64) (*registry.ServiceURL, error)

var (
	ErrNotFound              = errors.New("not found")
	ErrNoneAvailable         = errors.New("none available")
	ErrRunOutAllServiceNodes = errors.New("has used out all provider nodes")
)

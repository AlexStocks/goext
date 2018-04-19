package selector

import (
	// "golang.org/x/net/context"
	"context"
)

import (
	"github.com/AlexStocks/dubbogo/registry"
)

type Options struct {
	Registry registry.Registry
	Mode     SelectorMode // selector mode

	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

// Option used to initialise the selector
type Option func(*Options)

// Registry sets the registry used by the selector
func Registry(r registry.Registry) Option {
	return func(o *Options) {
		o.Registry = r
	}
}

// SetStrategy sets the default strategy for the selector
func SelectMode(mode SelectorMode) Option {
	return func(o *Options) {
		o.Mode = mode
	}
}

func Context(ctx context.Context) Option {
	return func(o *Options) {
		o.Context = ctx
	}
}

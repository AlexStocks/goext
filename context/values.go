package gxcontext

import (
	"context"
)

var (
	defaultCtxKey int = 1
)

type Values struct {
	m map[interface{}]interface{}
}

func (v Values) Get(key interface{}) interface{} {
	return v.m[key]
}

func (c Values) Set(key interface{}, value interface{}) {
	c.m[key] = value
}

func (c Values) Delete(key interface{}) {
	delete(c.m, key)
}

type ValuesContext struct {
	context.Context
}

func NewValuesContext(ctx context.Context) *ValuesContext {
	if ctx == nil {
		ctx = context.Background()
	}

	return &ValuesContext{
		Context: context.WithValue(
			ctx,
			defaultCtxKey,
			Values{m: make(map[interface{}]interface{})},
		),
	}
}

func (c *ValuesContext) Get(key interface{}) interface{} {
	return c.Context.Value(defaultCtxKey).(Values).Get(key)
}

func (c *ValuesContext) Delete(key interface{}) {
	c.Context.Value(defaultCtxKey).(Values).Delete(key)
}

func (c *ValuesContext) Set(key interface{}, value interface{}) {
	c.Context.Value(defaultCtxKey).(Values).Set(key, value)
}

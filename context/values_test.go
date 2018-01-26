package gxcontext

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestNewValuesContext(t *testing.T) {
	xassert := assert.New(t)

	ctx := NewValuesContext(nil)
	key := "hello key"
	value := "hello value"
	ctx.Set(key, value)
	v := ctx.Get(key)
	xassert.Equalf(value, v, "key:%s", key)

	ctx1 := NewValuesContext(nil)
	key1 := struct {
		s string
		i int
	}{s: "hello key", i: 100}
	ctx1.Set(key1, value)
	v1 := ctx1.Get(key1)
	xassert.Equalf(value, v1, "key:%#v", key1)
}

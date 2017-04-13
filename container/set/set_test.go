package gxset

import (
	"fmt"
	"testing"
)

import (
	"github.com/davecgh/go-spew/spew"
)

func assertEqual(a, b Set, t *testing.T) {
	if !a.Equal(b) {
		t.Errorf("%v != %v\n", a, b)
	}
}

func TestNewSetFromMapKey(t *testing.T) {
	var m = map[interface{}]interface{}{
		"k0": "v0",
		"k1": "v1",
		"k2": "v2",
		"k3": "v3",
		"k4": "v4",
	}
	s := NewSetFromMapKey(m)
	if s.Cardinality() == 0 {
		t.Error("NewSetFromMapKey should not start out as an empty set")
	}

	fmt.Printf("set:%s\n", spew.Sdump(s.ToSlice()))

	var a = []string{"k0", "k1", "k10"}
	as := NewSet()
	for _, item := range a {
		as.Add(item)
	}
	fmt.Printf("as:%s\n", spew.Sdump(as.ToSlice()))

	fmt.Printf("intersect:%s\n", spew.Sdump(s.Intersect(as)))
	fmt.Printf("diff:%s\n", spew.Sdump(s.Difference(as)))
	fmt.Printf("diff:%s\n", spew.Sdump(s.Union(as)))
}

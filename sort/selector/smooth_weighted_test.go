package gxselector

// ref: https://github.com/smallnest/weighted/blob/master/smooth_weighted_test.go

import (
	"testing"
)

func TestSW_Next(t *testing.T) {
	w := NewSW(3)
	w.Add("server1", 5)
	w.Add("server2", 2)
	w.Add("server3", 3)

	results := make(map[string]int)

	for i := 0; i < 100; i++ {
		it := w.Next()
		if it != nil {
			item := it.Item().(string)
			results[item]++
		}
	}

	if results["server1"] != 50 || results["server2"] != 20 || results["server3"] != 30 {
		t.Error("the algorithm is wrong")
	}

	w.Reset()
	results = make(map[string]int)

	for i := 0; i < 100; i++ {
		it := w.Next()
		if it != nil {
			item := it.Item().(string)
			results[item]++
		}
	}

	if results["server1"] != 50 || results["server2"] != 20 || results["server3"] != 30 {
		t.Error("the algorithm is wrong")
	}

	w.RemoveAll()
	w.Add("server1", 7)
	w.Add("server2", 9)
	w.Add("server3", 13)

	results = make(map[string]int)

	for i := 0; i < 29000; i++ {
		it := w.Next()
		if it != nil {
			item := it.Item().(string)
			results[item]++
		}
	}

	if results["server1"] != 7000 || results["server2"] != 9000 || results["server3"] != 13000 {
		t.Error("the algorithm is wrong")
	}
}

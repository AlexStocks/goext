// Copyright 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// From the paper "A Fast, Minimal Memory, Consistent Hash Algorithm" by John Lamping, Eric Veach (2014).
// http://arxiv.org/abs/1406.2294
package gxjump

const (
	amplify = uint64(2862933555777941757)
)

func JumpConsistentHash(key uint64, num_buckets int) int32 {
	var b = int64(-1)
	var j = int64(0)

	if num_buckets <= 0 {
		num_buckets = 1
	}
	for j < int64(num_buckets) {
		b = j
		key = key*amplify + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}

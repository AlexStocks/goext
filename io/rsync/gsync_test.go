// This Source Code Form is subject to the terms of the Mozilla Public
// License, version 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package gxrsync implements a rsync-based algorithm for sending delta updates to a remote server.
// ref: github.com/c4milo/gsync
package gxrsync

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"

	"github.com/hooklift/assert"
	"github.com/pkg/profile"
)

// TestRollingHash tests that incrementally calculated signatures arrive to the same
// value as the full block signature.
func TestRollingHash(t *testing.T) {
	_, _, target := rollingHash([]byte("abcd"))          // file's content in server
	reader := bytes.NewReader([]byte("aaabcdbbabcdddf")) // new file's content in client

	var (
		r1, r2, r, old uint32
		offset         int64
		rolling        bool
	)

	delta := make([]byte, 0)
	for {
		buffer := make([]byte, 4) // block size of 4
		n, err := reader.ReadAt(buffer, offset)

		block := buffer[:n]
		if rolling {
			new := uint32(block[n-1])
			r1, r2, r = rollingHash2(uint32(n), r1, r2, old, new)
		} else {
			r1, r2, r = rollingHash(block)
		}

		if r == target {
			if err == io.EOF {
				break
			}

			rolling = false
			old, r, r1, r2 = 0, 0, 0, 0
			offset += int64(n)
		} else {
			if err == io.EOF {
				// If EOF is reached and not matched data is found, we need to add trailing data
				// to delta array.
				delta = append(delta, block...)
				break
			}

			rolling = true
			old = uint32(block[0])
			delta = append(delta, block[0])
			offset++
		}

		assert.Ok(t, err)
	}

	assert.Equals(t, []byte("aabbddf"), delta)
}

var alpha = "abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789\n"

// srand generates a random string of fixed size.
func srand(seed int64, size int) []byte {
	buf := make([]byte, size)
	rand.Seed(seed)
	for i := 0; i < size; i++ {
		buf[i] = alpha[rand.Intn(len(alpha))]
	}
	return buf
}

func TestSync(t *testing.T) {
	defer profile.Start().Stop()
	tests := []struct {
		desc   string
		source []byte
		cache  []byte
		h      hash.Hash
	}{
		{
			"full sync, no cache, 2mb file",
			srand(10, (2*1024)*1024),
			nil,
			md5.New(),
		},
		{
			"partial sync, 1mb cached, 2mb file",
			srand(20, (2*1024)*1024),
			srand(20, (1*1024)*1024),
			md5.New(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			if len(tt.cache) > 0 {
				assert.Equals(t, tt.source[:len(tt.cache)], tt.cache)
			}

			fmt.Print("Signatures... ")
			sigsCh, err := Signatures(ctx, bytes.NewReader(tt.cache), tt.h)
			assert.Ok(t, err)
			fmt.Println("done")

			fmt.Print("LookUpTable... ")
			cacheSigs, err := LookUpTable(ctx, sigsCh)
			assert.Ok(t, err)
			fmt.Printf("%d blocks found in cache. done\n", len(cacheSigs))

			fmt.Print("Sync... ")
			opsCh, err := Sync(ctx, bytes.NewReader(tt.source), tt.h, cacheSigs)
			assert.Ok(t, err)
			fmt.Println("done")

			fmt.Print("Apply... ")
			target := new(bytes.Buffer)
			err = Apply(ctx, target, bytes.NewReader(tt.cache), opsCh)
			assert.Ok(t, err)
			fmt.Println("done")

			assert.Cond(t, target.Len() != 0, "target file should not be empty")
			if !bytes.Equal(tt.source, target.Bytes()) {
				ioutil.WriteFile("source.txt", tt.source, 0640)
				ioutil.WriteFile("cache.txt", tt.cache, 0640)
				ioutil.WriteFile("target.txt", target.Bytes(), 0640)
			}
			assert.Cond(t, bytes.Equal(tt.source, target.Bytes()), "source and target files are different")
		})
	}
}

func Benchmark6kbBlockSize(b *testing.B)    {}
func Benchmark128kbBlockSize(b *testing.B)  {}
func Benchmark512kbBlockSize(b *testing.B)  {}
func Benchmark1024kbBlockSize(b *testing.B) {}

func BenchmarkMD5(b *testing.B)     {}
func BenchmarkSHA256(b *testing.B)  {}
func BenchmarkSHA512(b *testing.B)  {}
func BenchmarkMurmur3(b *testing.B) {}
func BenchmarkXXHash(b *testing.B)  {}

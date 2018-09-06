// package bytebuf
package gxbuffer

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

var sinkString string

func BenchmarkString(b *testing.B) {
	args := []string{
		"",
		"Intel",
		strings.Repeat("x", 64),
		strings.Repeat("x", 128),
		strings.Repeat("x", 1024),
	}

	for _, arg := range args {
		name := "empty"
		if arg != "" {
			name = fmt.Sprint(len(arg))
		}

		b.Run("bytebuf.Buffer/"+name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				buf := New()
				buf.WriteString(arg)
				sinkString = buf.String()
			}
		})
		b.Run("bytes.Buffer/"+name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var buf bytes.Buffer
				buf.WriteString(arg)
				sinkString = buf.String()
			}
		})
	}
}

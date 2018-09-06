## bytebuf

Replacement for [bytes.Buffer](https://golang.org/pkg/bytes/#Buffer) that you can use in a performance-sensitive parts
or your Go programs.

For explanation why this package is needed, see [rationale](#implementation-difference--rationale).

## Quick start / Installation

```bash
go get -u github.com/intel-go/bytebuf
```

The only difference from `bytes.Buffer` is explicit constructor:

```go
buf := bytebuf.New() // Can't just use zero value
// Now can use as an ordinary bytes.Buffer:
buf.WriteString("123")
buf.Write([]byte("456")
```

## Performance

Given this code:

```
buf.Write(s)
globalString = buf.String()
```

Where `s` is:

| Label | Data |
|-------|------|
| empty | ""   |
| 5     | "Intel" |
| 64    | 64-byte string |
| 128   | 128-byte string |
| 1024  | 1024-byte string |

```
name            old time/op    new time/op    delta
String/empty-8     138ns ±13%      24ns ± 0%   -82.94%  (p=0.000 n=10+8)
String/5-8         186ns ±11%      60ns ± 1%   -67.82%  (p=0.000 n=10+10)
String/64-8        225ns ±10%     108ns ± 6%   -52.26%  (p=0.000 n=10+10)
String/128-8       474ns ±17%     338ns ±13%   -28.57%  (p=0.000 n=10+10)
String/1024-8      889ns ± 0%     740ns ± 1%   -16.78%  (p=0.000 n=9+10)

name            old alloc/op   new alloc/op   delta
String/empty-8      112B ± 0%        0B       -100.00%  (p=0.000 n=10+10)
String/5-8          117B ± 0%        5B ± 0%   -95.73%  (p=0.000 n=10+10)
String/64-8         176B ± 0%       64B ± 0%   -63.64%  (p=0.000 n=10+10)
String/128-8        368B ± 0%      256B ± 0%   -30.43%  (p=0.000 n=10+10)
String/1024-8     2.16kB ± 0%    2.05kB ± 0%    -5.19%  (p=0.000 n=10+10)

name            old allocs/op  new allocs/op  delta
String/empty-8      1.00 ± 0%      0.00       -100.00%  (p=0.000 n=10+10)
String/5-8          2.00 ± 0%      1.00 ± 0%   -50.00%  (p=0.000 n=10+10)
String/64-8         2.00 ± 0%      1.00 ± 0%   -50.00%  (p=0.000 n=10+10)
String/128-8        3.00 ± 0%      2.00 ± 0%   -33.33%  (p=0.000 n=10+10)
String/1024-8       3.00 ± 0%      2.00 ± 0%   -33.33%  (p=0.000 n=10+10)
```

Note: it requres [CL133375](https://golang.org/cl/133375) to be applied to your Go compiler.

## Implementation difference / Rationale

The whole implementation difference can be described as:

```diff
type Buffer struct {
	buf       []byte   // contents are the bytes buf[off : len(buf)]
	off       int      // read at &buf[off], write at &buf[len(buf)]
- 	bootstrap [64]byte // memory to hold first slice; helps small buffers avoid allocation.
+ 	bootstrap *[64]byte // memory to hold first slice; helps small buffers avoid allocation.
	lastRead  readOp   // last read operation, so that Unread* can work correctly.
}
```

With updated escape analysis, it's possible to actually take benefits of
bootstrap array, but only if it's not "inlined" into `Buffer` object.
So, we need a pointer to array instead of normal array.

This makes it impossible to use zero value though, hence `New` function.
Unfortunately, it means that we can't merge this into golang `bytes` package,
it would be a breaking change in package API.

If you're interested in bootstrap array problem, take a look at
[cmd/compile, bytes: bootstrap array causes bytes.Buffer to always be heap-allocated](https://github.com/golang/go/issues/7921).

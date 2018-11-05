// An implementation of Consistent Hashing and
// Consistent Hashing With Bounded Loads.
//
// https://en.wikipedia.org/wiki/ConsistentHash_hashing
//
// https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
package gxconsistent

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

import (
	"github.com/AlexStocks/goext/strings"
	jerrors "github.com/juju/errors"
	blake2b "github.com/minio/blake2b-simd"
)

const (
	replicationFactor = 10
	maxBucketNum      = math.MaxUint32
)

var (
	ErrNoHosts = jerrors.New("no hosts added")
)

type Options struct {
	HashFunc    HashFunc
	ReplicaNum  int
	MaxVnodeNum int
}

type Option func(*Options)

func WithHashFunc(hash HashFunc) Option {
	return func(opts *Options) {
		opts.HashFunc = hash
	}
}

func WithReplicaNum(replicaNum int) Option {
	return func(opts *Options) {
		opts.ReplicaNum = replicaNum
	}
}

func WithMaxVnodeNum(maxVnodeNum int) Option {
	return func(opts *Options) {
		opts.MaxVnodeNum = maxVnodeNum
	}
}

type hashArray []uint32

// Len returns the length of the hashArray.
func (x hashArray) Len() int { return len(x) }

// Less returns true if element i is less than element j.
func (x hashArray) Less(i, j int) bool { return x[i] < x[j] }

// Swap exchanges elements i and j.
func (x hashArray) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

type Host struct {
	Name string
	Load int64
}

type HashFunc func([]byte) uint64

func hash(key []byte) uint64 {
	out := blake2b.Sum512(key)
	return binary.LittleEndian.Uint64(out[:])
}

type ConsistentHash struct {
	circle        map[uint32]string // hash -> node name
	sortedHashes  hashArray         // hash valid in ascending
	loadMap       map[string]*Host  // node name -> struct Host
	totalLoad     int64             // total load
	replicaFactor uint32
	bucketNum     uint32
	hashFunc      HashFunc

	sync.RWMutex
}

func NewConsistentHash(opts ...Option) *ConsistentHash {
	var options Options

	for idx := range opts {
		opts[idx](&options)
	}

	if options.ReplicaNum <= 0 {
		options.ReplicaNum = replicationFactor
	}
	if options.MaxVnodeNum <= 0 {
		options.MaxVnodeNum = maxBucketNum
	}
	if options.HashFunc == nil {
		options.HashFunc = hash
	}
	return &ConsistentHash{
		circle:        map[uint32]string{},
		loadMap:       map[string]*Host{},
		replicaFactor: uint32(options.ReplicaNum),
		bucketNum:     uint32(options.MaxVnodeNum),
		hashFunc:      options.HashFunc,
	}
}

func (c *ConsistentHash) SetHashFunc(f HashFunc) {
	c.hashFunc = f
}

// eltKey generates a string key for an element with an index.
func (c *ConsistentHash) eltKey(elt string, idx int) string {
	// return elt + "|" + strconv.Itoa(idx)
	return strconv.Itoa(idx) + elt
}

func (c *ConsistentHash) hash(key string) uint32 {
	return uint32(c.hashFunc(gxstrings.Slice(key))) % c.bucketNum
}

// sort hashes in ascending
func (c *ConsistentHash) updateSortedHashes() {
	hashes := c.sortedHashes[:0]
	//reallocate if we're holding on to too much (1/4th)
	if c.sortedHashes.Len()/int(c.replicaFactor*4) > len(c.circle) {
		hashes = nil
	}
	for k := range c.circle {
		hashes = append(hashes, uint32(k))
	}
	sort.Sort(hashes)
	c.sortedHashes = hashes
}

func (c *ConsistentHash) Add(host string) {
	c.Lock()
	defer c.Unlock()
	c.add(host)
}

func (c *ConsistentHash) add(host string) {
	if _, ok := c.loadMap[host]; ok {
		return
	}

	c.loadMap[host] = &Host{Name: host}
	for i := uint32(0); i < c.replicaFactor; i++ {
		h := c.hash(c.eltKey(host, int(i)))
		c.circle[h] = host
		c.sortedHashes = append(c.sortedHashes, uint32(h))
	}

	// sort.Slice(c.sortedHashes, func(i int, j int) bool {
	// 	if c.sortedHashes[i] < c.sortedHashes[j] {
	// 		return true
	// 	}
	// 	return false
	// })
	c.updateSortedHashes()
}

// Set sets all the elements in the hash. If there are existing elements not
// present in elts, they will be removed.
func (c *ConsistentHash) Set(elts []string) {
	c.Lock()
	defer c.Unlock()
	for k := range c.loadMap {
		found := false
		for _, v := range elts {
			if k == v {
				found = true
				break
			}
		}
		if !found {
			c.remove(k)
		}
	}
	for _, v := range elts {
		_, exists := c.loadMap[v]
		if exists {
			continue
		}
		c.add(v)
	}
}

func (c *ConsistentHash) Members() []string {
	c.RLock()
	defer c.RUnlock()
	var m []string
	for k := range c.loadMap {
		m = append(m, k)
	}
	return m
}

// Returns the host that owns `key`.
//
// As described in https://en.wikipedia.org/wiki/ConsistentHash_hashing
//
// It returns ErrNoHosts if the ring has no hosts in it.
func (c *ConsistentHash) Get(key string) (string, error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.circle) == 0 {
		return "", ErrNoHosts
	}

	h := c.hash(key)
	idx := c.search(h)
	return c.circle[c.sortedHashes[idx]], nil
}

// Returns the host that owns `hashKey`.
//
// It returns ErrNoHosts if the ring has no hosts in it.
func (c *ConsistentHash) GetHash(hashKey uint32) (string, error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.circle) == 0 {
		return "", ErrNoHosts
	}

	idx := c.search(hashKey)
	return c.circle[c.sortedHashes[idx]], nil
}

// GetTwo returns the two closest distinct elements to the name input in the circle.
func (c *ConsistentHash) GetTwo(name string) (string, string, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.circle) == 0 {
		return "", "", ErrNoHosts
	}
	key := c.hash(name)
	i := c.search(key)
	a := c.circle[c.sortedHashes[i]]

	if len(c.loadMap) == 1 {
		return a, "", nil
	}

	start := i
	var b string
	for i = start + 1; i != start; i++ {
		if i >= len(c.sortedHashes) {
			i = 0
		}
		b = c.circle[c.sortedHashes[i]]
		if b != a {
			break
		}
	}
	return a, b, nil
}

func sliceContainsMember(set []string, member string) bool {
	for _, m := range set {
		if m == member {
			return true
		}
	}
	return false
}

// GetN returns the N closest distinct elements to the name input in the circle.
func (c *ConsistentHash) GetN(name string, n int) ([]string, error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.circle) == 0 {
		return nil, ErrNoHosts
	}

	if len(c.loadMap) < n {
		n = int(len(c.loadMap))
	}

	var (
		key   = c.hash(name)
		i     = c.search(key)
		start = i
		res   = make([]string, 0, n)
		elem  = c.circle[c.sortedHashes[i]]
	)

	res = append(res, elem)

	if len(res) == n {
		return res, nil
	}

	for i = start + 1; i != start; i++ {
		if i >= len(c.sortedHashes) {
			i = 0
		}
		elem = c.circle[c.sortedHashes[i]]
		if !sliceContainsMember(res, elem) {
			res = append(res, elem)
		}
		if len(res) == n {
			break
		}
	}

	return res, nil
}

// It uses ConsistentHash Hashing With Bounded loads
//
// https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
//
// to pick the least loaded host that can serve the key
//
// It returns ErrNoHosts if the ring has no hosts in it.
//
func (c *ConsistentHash) GetLeast(key string) (string, error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.circle) == 0 {
		return "", ErrNoHosts
	}

	h := c.hash(key)
	idx := c.search(h)

	i := idx
	for {
		host := c.circle[c.sortedHashes[i]]
		if c.loadOK(host) {
			return host, nil
		}
		i++
		if i >= len(c.circle) {
			i = 0
		}
	}
}

func (c *ConsistentHash) search(key uint32) int {
	idx := sort.Search(len(c.sortedHashes), func(i int) bool {
		return c.sortedHashes[i] >= key
	})

	if idx >= len(c.sortedHashes) {
		idx = 0
	}
	return idx
}

// Sets the load of `host` to the given `load`
func (c *ConsistentHash) UpdateLoad(host string, load int64) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.loadMap[host]; !ok {
		return
	}
	c.totalLoad -= c.loadMap[host].Load
	c.loadMap[host].Load = load
	c.totalLoad += load
}

// Increments the load of host by 1
//
// should only be used with if you obtained a host with GetLeast
func (c *ConsistentHash) Inc(host string) {
	atomic.AddInt64(&c.loadMap[host].Load, 1)
	atomic.AddInt64(&c.totalLoad, 1)
}

// Decrements the load of host by 1
//
// should only be used with if you obtained a host with GetLeast
func (c *ConsistentHash) Done(host string) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.loadMap[host]; !ok {
		return
	}
	atomic.AddInt64(&c.loadMap[host].Load, -1)
	atomic.AddInt64(&c.totalLoad, -1)
}

// Deletes host from the ring
func (c *ConsistentHash) Remove(host string) bool {
	c.Lock()
	defer c.Unlock()
	return c.remove(host)
}

func (c *ConsistentHash) remove(host string) bool {
	for i := uint32(0); i < c.replicaFactor; i++ {
		h := c.hash(c.eltKey(host, int(i)))
		delete(c.circle, h)
		c.delSlice(h)
	}
	if _, ok := c.loadMap[host]; ok {
		atomic.AddInt64(&c.totalLoad, -c.loadMap[host].Load)
		delete(c.loadMap, host)
	}
	return true
}

// Return the list of hosts in the ring
func (c *ConsistentHash) Hosts() (hosts []string) {
	c.RLock()
	defer c.RUnlock()
	for k, _ := range c.loadMap {
		hosts = append(hosts, k)
	}
	return hosts
}

// Returns the loads of all the hosts
func (c *ConsistentHash) GetLoads() map[string]int64 {
	loads := map[string]int64{}

	for k, v := range c.loadMap {
		loads[k] = v.Load
	}
	return loads
}

// Returns the maximum load of the single host
// which is:
// (total_load/number_of_hosts)*1.25
// total_load = is the total number of active requests served by hosts
// for more info:
// https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
func (c *ConsistentHash) MaxLoad() int64 {
	if c.totalLoad == 0 {
		c.totalLoad = 1
	}
	var avgLoadPerNode float64
	avgLoadPerNode = float64(c.totalLoad / int64(len(c.loadMap)))
	if avgLoadPerNode == 0 {
		avgLoadPerNode = 1
	}
	avgLoadPerNode = math.Ceil(avgLoadPerNode * 1.25)
	return int64(avgLoadPerNode)
}

func (c *ConsistentHash) loadOK(host string) bool {
	// a safety check if someone performed c.Done more than needed
	if c.totalLoad < 0 {
		c.totalLoad = 0
	}

	var avgLoadPerNode float64
	avgLoadPerNode = float64((c.totalLoad + 1) / int64(len(c.loadMap)))
	if avgLoadPerNode == 0 {
		avgLoadPerNode = 1
	}
	avgLoadPerNode = math.Ceil(avgLoadPerNode * 1.25)

	bhost, ok := c.loadMap[host]
	if !ok {
		panic(fmt.Sprintf("given host(%s) not in loadsMap", bhost.Name))
	}

	if float64(bhost.Load)+1 <= avgLoadPerNode {
		return true
	}

	return false
}

func (c *ConsistentHash) delSlice(val uint32) {
	for i := 0; i < len(c.sortedHashes); i++ {
		if c.sortedHashes[i] == val {
			c.sortedHashes = append(c.sortedHashes[:i], c.sortedHashes[i+1:]...)
		}
	}
}

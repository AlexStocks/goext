// Copyright 2016 ~ 2017 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

package gxsync

// refs: https://github.com/orcaman/concurrent-map/blob/master/concurrent_map.go
// Please do not use it. Just wait the official release in G0 1.9 or 1.10 to wait the method (Map)Len() int
import (
	"sync"
)

var SHARD_COUNT = 32

type Hash func(key interface{}) uint32

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (m.shardNum) map shards.
type HashMap struct {
	shardNum int // shard number
	hash     Hash
	shard    []*Map // use pointer here. cause sync.*HashMap obj can not be copied.
}

// Creates a new concurrent map.
func NewHashMap(shardNum int, hash Hash) *HashMap {
	if shardNum < SHARD_COUNT {
		shardNum = SHARD_COUNT
	}

	m := &HashMap{shardNum: shardNum, hash: hash, shard: make([]*Map, shardNum)}
	for i := 0; i < shardNum; i++ {
		m.shard[i] = &Map{}
	}

	return m
}

// Returns shard under given key
func (m *HashMap) GetShard(key interface{}) *Map {
	return m.shard[uint(m.hash(key))%uint(m.shardNum)]
}

func (m *HashMap) MSet(data map[string]interface{}) {
	for key, value := range data {
		m.Set(key, value)
	}
}

// Sets the given value under the specified key.
func (m *HashMap) Set(key interface{}, value interface{}) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Store(key, value)
}

// Callback to return new element to be inserted into the map
// It is called while lock is held, therefore it MUST NOT
// try to access other keys in same map, as it can lead to deadlock since
// Go sync.RWLock is not reentrant
type UpsertCb func(exist bool, valueInMap interface{}, newValue interface{}) interface{}

// Insert or Update - updates existing element or inserts a new one using UpsertCb
func (m *HashMap) Upsert(key interface{}, value interface{}, cb UpsertCb) (res interface{}) {
	shard := m.GetShard(key)
	v, ok := shard.Load(key)
	res = cb(ok, v, value)
	shard.Store(key, res)

	return res
}

// Sets the given value under the specified key if no value was associated with it.
// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *HashMap) SetIfAbsent(key interface{}, value interface{}) bool {
	// Get map shard.
	shard := m.GetShard(key)
	_, ok := shard.LoadOrStore(key, value)
	return !ok
}

// Sets the given value under the specified key if oldValue was associated with it.
func (m *HashMap) SetIfPresent(key interface{}, newValue, oldValue interface{}) bool {
	// Get map shard.
	shard := m.GetShard(key)
	v, ok := shard.Load(key)
	ok = ok && v == oldValue
	if ok {
		shard.Store(key, newValue)
	}

	return ok
}

// Retrieves an element from map under given key.
func (m *HashMap) Get(key interface{}) (interface{}, bool) {
	// Get shard
	shard := m.GetShard(key)
	return shard.Load(key)
}

// Returns the number of elements within the map.
func (m *HashMap) Count() int {
	//return int(atomic.LoadInt64(&(m.size)))
	// Returns the number of elements within the map.
	count := 0
	for i := 0; i < m.shardNum; i++ {
		count += m.shard[i].Len()
	}

	return count
}

// Looks up an item under specified key
func (m *HashMap) Has(key interface{}) bool {
	// Get shard
	shard := m.GetShard(key)
	_, ok := shard.Load(key)

	return ok
}

// Removes an element from the map.
func (m *HashMap) Remove(key interface{}) {
	// Try to get shard.
	shard := m.GetShard(key)
	if _, ok := shard.Load(key); ok {
		shard.Delete(key)
	}
}

// Removes an element from the map and returns it
func (m *HashMap) Pop(key interface{}) (v interface{}, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	v, ok := shard.Load(key)
	if ok {
		shard.Delete(key)
	}
	return v, ok
}

// Checks if map is empty.
func (m *HashMap) IsEmpty() bool {
	return m.Count() == 0
}

// Used by the Iter & IterBuffered functions to wrap two variables together over a channel,
type Tuple struct {
	Key, Val interface{}
}

// Returns an iterator which could be used in a for range loop.
//
// Deprecated: using IterBuffered() will get a better performence
func (m *HashMap) Iter() <-chan Tuple {
	return m.IterBuffered()
}

// Returns a buffered iterator which could be used in a for range loop.
func (m *HashMap) IterBuffered() <-chan Tuple {
	ch := make(chan Tuple, m.Count())
	go func() {
		wg := sync.WaitGroup{}
		wg.Add(m.shardNum)
		// Foreach shard.
		for _, shard := range m.shard {
			go func(shard *Map) {
				// Foreach key, value pair.
				shard.Range(func(key, value interface{}) bool {
					ch <- Tuple{key, value}
					return true
				})
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()
	return ch
}

// Returns all items as map[string]interface{}
func (m *HashMap) Items() map[interface{}]interface{} {
	items := make(map[interface{}]interface{})

	for item := range m.IterBuffered() {
		items[item.Key] = item.Val
	}

	return items
}

// Iterator callback,called for every key,value found in
// maps. RLock is held for all calls for a given shard
// therefore callback sess consistent view of a shard,
// but not across the shards
type IterCb func(key interface{}, v interface{}) bool

// Callback based iterator, cheapest way to read
// all elements in a map.
func (m *HashMap) IterCb(fn IterCb) {
	for idx := range m.shard {
		shard := m.shard[idx]
		shard.Range(fn)
	}
}

// Return all keys as []string
func (m *HashMap) Keys() []interface{} {
	count := m.Count()
	ch := make(chan interface{}, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(m.shardNum)
		for _, shard := range m.shard {
			go func(shard *Map) {
				// Foreach key, value pair.
				shard.Range(func(key, value interface{}) bool {
					ch <- key
					return true
				})
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	// Generate keys
	keys := make([]interface{}, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}

	return keys
}

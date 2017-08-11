// Copyright 2016 ~ 2017 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// refers to https://github.com/golang/go/blob/master/src/sync/map.go
// package sync
// Please do not use it. Just wait the official release in G0 1.9 or 1.10 to wait the method (Map)Len() int
package gxsync

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// Map is a concurrent map with amortized-constant-time loads, stores, and deletes.
// It is safe for multiple goroutines to call a Map's methods concurrently.
//
// The zero Map is valid and empty.
//
// A Map must not be copied after first use.
// 如果ditry不为空，则包含read中全部值，如果为空则说明老的ditry刚被提升为read
type Map struct {
	// 当涉及到dirty数据的操作的时候，需要使用这个锁
	mu sync.Mutex

	// 一个只读的数据结构，因为只读，所以不会有读写冲突。
	// 所以从这个数据中读取总是安全的。
	//
	// 实际上，实际也会更新这个数据的entries,如果entry是未删除的(unexpunged),
	// 并不需要加锁。如果entry已经被删除了，需要加锁，以便更新dirty数据。
	read atomic.Value // readOnly

	// dirty数据包含当前的map包含的entries,它包含最新的entries(包括read中未删除的数据,
	// 虽有冗余，但是提升dirty字段为read的时候非常快，不用一个一个的复制，
	// 而是直接将这个数据结构作为read字段的一部分),有些数据还可能没有移动到read字段中。
	//
	// 对于dirty的操作需要加锁，因为对它的操作可能会有读写竞争。
	//
	// 当dirty为空的时候， 比如初始化或者刚提升完，下一次的写操作会复制read字段中未删除的数据到这个数据中。
	dirty map[interface{}]*entry

	// 当从Map中读取entry的时候，如果read中不包含这个entry,会尝试从dirty中读取，
	// 这个时候会将misses加一，
	// 当misses累积到 dirty的长度的时候， 就会将dirty提升为read,避免从dirty中miss太多次。
	// 因为操作dirty需要加锁。
	misses int

	// map element size
	size int64
}

// readOnly is an immutable struct stored atomically in the Map.read field.
// amended指明Map.dirty中有readOnly.m未包含的数据，所以如果从Map.read找不到数据的话，
// 还要进一步到Map.dirty中查找。
// 对Map.read的修改是通过原子操作进行的。
//
// 虽然read和dirty有冗余数据，但这些数据是通过指针指向同一个数据，所以尽管Map的value会很大，
// 但是冗余的空间占用还是有限的。
type readOnly struct {
	m       map[interface{}]*entry
	amended bool // 如果Map.dirty有些数据不在中的时候，这个值为true
}

// expunged is an arbitrary pointer that marks entries which have been deleted
// from the dirty map.
var expunged = unsafe.Pointer(new(interface{}))

// An entry is a slot in the map corresponding to a particular key.
// entry是map中value的代理，它是一个一级指针
// 不管是read.m还是dirty，其值都是*entry，是一个二级指针
type entry struct {
	// p有三种值：
	// nil: entry已被删除了，并且m.dirty为nil
	// expunged: entry已被删除了，并且m.dirty不为nil，而且这个entry不存在于m.dirty中
	// 其它： entry是一个正常的值, 如果m.dirty不为空，则m.dirty[key]指向该值，read中不一定存在
	p unsafe.Pointer // *interface{}
}

func newEntry(i interface{}) *entry {
	return &entry{p: unsafe.Pointer(&i)}
}

func (m *Map) missLocked() {
	m.misses++
	if m.misses < len(m.dirty) {
		return
	}
	// 上面的最后三行代码就是提升m.dirty的，很简单的将m.dirty作为readOnly的m字段，
	// 原子更新m.read。提升后m.dirty、m.misses重置， 并且m.read.amended为false。
	m.read.Store(readOnly{m: m.dirty})
	m.dirty = nil
	m.misses = 0
}

func (m *Map) Len() int {
	return int(atomic.LoadInt64(&(m.size)))
}

// 双检查的技术Java程序员非常熟悉了，单例模式的实现之一就是利用双检查的技术。
//
// 可以看到，如果我们查询的键值正好存在于m.read中，无须加锁，直接返回，理论上性能优异。
// 即使不存在于m.read中，经过miss几次之后，m.dirty会被提升为m.read，又会从m.read中查找。
// 所以对于更新／增加较少，加载存在的key很多的case,性能基本和无锁的map类似。
func (m *Map) Load(key interface{}) (value interface{}, ok bool) {
	// 1.首先从m.read中得到只读readOnly,从它的map中查找，不需要加锁
	read, _ := m.read.Load().(readOnly)
	e, ok := read.m[key]
	// 2. 如果没找到，并且m.dirty中有新数据，需要从m.dirty查找，这个时候需要加锁
	if !ok && read.amended {
		m.mu.Lock()
		// 双检查，避免加锁的时候m.dirty提升为m.read,这个时候m.read可能被替换了。
		read, _ = m.read.Load().(readOnly)
		e, ok = read.m[key]
		// 如果m.read中还是不存在，并且m.dirty中有新数据
		if !ok && read.amended {
			// 从m.dirty查找
			e, ok = m.dirty[key]
			// 不管m.dirty中存不存在，都将misses计数加一
			// missLocked()中满足条件后就会提升m.dirty
			m.missLocked()
		}
		m.mu.Unlock()
	}
	if !ok {
		return nil, false
	}

	// 就算read和dirty中key存在，其value也肯能为nil，请参见Delete函数
	return e.load()
}

func (e *entry) load() (value interface{}, ok bool) {
	// 只加载指针，然后读取相应值
	// 如果使用atomic.Load，则进行了值拷贝，浪费时间
	p := atomic.LoadPointer(&e.p)
	if p == nil || p == expunged {
		return nil, false
	}
	return *(*interface{})(p), true
}

// tryStore尝试把值更新到read中，更新一个尚未被删除的entry的value
//
// 如果已经被标记为expunged，则说明dirty存在，则不再尝试把值存到read中，而是写到dirty中
func (e *entry) tryStore(i *interface{}) bool {
	p := atomic.LoadPointer(&e.p)
	if p == expunged {
		return false
	}
	for {
		if atomic.CompareAndSwapPointer(&e.p, p, unsafe.Pointer(i)) {
			return true
		}
		p = atomic.LoadPointer(&e.p)
		if p == expunged {
			return false
		}
	}
}

// storeLocked无条件地把value存到entry中
//
// entry原来的值不能是expunged
func (e *entry) storeLocked(i *interface{}) {
	atomic.StorePointer(&e.p, unsafe.Pointer(i))
}

// 标记entry的value为expunged
func (e *entry) tryExpungeLocked() (isExpunged bool) {
	p := atomic.LoadPointer(&e.p)
	for p == nil {
		if atomic.CompareAndSwapPointer(&e.p, nil, expunged) {
			return true
		}
		p = atomic.LoadPointer(&e.p)
	}
	return p == expunged
}

// 把read中的值copy到dirty中，如果read中相关值为nil，则把其值改为expunged
func (m *Map) dirtyLocked() {
	if m.dirty != nil {
		return
	}

	read, _ := m.read.Load().(readOnly)
	m.dirty = make(map[interface{}]*entry, len(read.m))
	for k, e := range read.m {
		if !e.tryExpungeLocked() {
			m.dirty[k] = e
		}
	}
}

// unexpungeLocked 返回false，说明以前并没有被标记为expunged.
//
// 如果已经被标记为expunged, 则说明dirty中也存了一分copy
func (e *entry) unexpungeLocked() (wasExpunged bool) {
	return atomic.CompareAndSwapPointer(&e.p, expunged, nil)
}

// Store sets the value for a key.
func (m *Map) Store(key, value interface{}) {
	///////////////////////
	// read存在，dirty不存在
	///////////////////////

	// 如果m.read存在这个键，并且这个entry没有被标记删除(dirty不存在)，尝试直接存储。
	read, _ := m.read.Load().(readOnly)
	if e, ok := read.m[key]; ok && e.tryStore(&value) {
		atomic.AddInt64(&(m.size), 1)
		return
	}

	///////////////////////
	// read存在，dirty存在
	///////////////////////
	// 进行到这里，说明`m.read`中不存在这个值或者已经被标记删除(expunged)
	m.mu.Lock()
	// 双重检查，可能dirty刚刚提升为read
	read, _ = m.read.Load().(readOnly)
	if e, ok := read.m[key]; ok { // read中存在这个值
		if e.unexpungeLocked() { // e.p:unpunged -> nil
			// read中存在这个值，dirty中也有这个值
			// 如果entry的值为expunged，说明dirty存在且相应值也为expunged，则要更新其entry
			m.dirty[key] = e
		}
		e.storeLocked(&value) // entry更新相应值
	} else if e, ok := m.dirty[key]; ok {
		// 相应值read中没有，dirty中存在
		e.storeLocked(&value) // 更新e的值，使其指向value即可
	} else {
		// read和dirty都不存在相应key对应的value
		if !read.amended {
			// dirty不存在，则创建复制read到dirty
			m.dirtyLocked() // 把read拷贝到dirty中
			m.read.Store(readOnly{m: read.m, amended: true})
		}
		m.dirty[key] = newEntry(value)
	}
	m.mu.Unlock()
	atomic.AddInt64(&(m.size), 1)
}

// tryLoadOrStore atomically loads or stores a value if the entry is not
// expunged.
//
// If the entry is expunged, tryLoadOrStore leaves the entry unchanged and
// returns with ok==false.
func (e *entry) tryLoadOrStore(i interface{}) (actual interface{}, loaded, ok bool) {
	p := atomic.LoadPointer(&e.p)
	if p == expunged {
		return nil, false, false
	}
	if p != nil {
		return *(*interface{})(p), true, true
	}

	// Copy the interface after the first load to make this method more amenable
	// to escape analysis: if we hit the "load" path or the entry is expunged, we
	// shouldn't bother heap-allocating.
	ic := i
	for {
		if atomic.CompareAndSwapPointer(&e.p, nil, unsafe.Pointer(&ic)) {
			return i, false, true
		}
		p = atomic.LoadPointer(&e.p)
		if p == expunged {
			return nil, false, false
		}
		if p != nil {
			return *(*interface{})(p), true, true
		}
	}
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *Map) LoadOrStore(key, value interface{}) (actual interface{}, loaded bool) {
	// Avoid locking if it's a clean hit.
	// 尝试从read中读取原值
	read, _ := m.read.Load().(readOnly)
	if e, ok := read.m[key]; ok {
		actual, loaded, ok := e.tryLoadOrStore(value)
		if ok {
			if !loaded {
				atomic.AddInt64(&(m.size), 1)
			}
			return actual, loaded
		}
	}

	m.mu.Lock()
	read, _ = m.read.Load().(readOnly)
	if e, ok := read.m[key]; ok {
		if e.unexpungeLocked() {
			m.dirty[key] = e
		}
		actual, loaded, ok = e.tryLoadOrStore(value)
		if ok && !loaded {
			atomic.AddInt64(&(m.size), 1)
		}
	} else if e, ok := m.dirty[key]; ok {
		actual, loaded, ok = e.tryLoadOrStore(value)
		if ok && !loaded {
			atomic.AddInt64(&(m.size), 1)
		}
		m.missLocked()
	} else {
		if !read.amended {
			// We're adding the first new key to the dirty map.
			// Make sure it is allocated and mark the read-only map as incomplete.
			m.dirtyLocked()
			m.read.Store(readOnly{m: read.m, amended: true})
			atomic.AddInt64(&(m.size), 1)
		}
		m.dirty[key] = newEntry(value)
		actual, loaded = value, false
	}
	m.mu.Unlock()

	return actual, loaded
}

func (e *entry) loadWithNil() (interface{}, bool) {
	p := atomic.LoadPointer(&e.p)
	if p == expunged {
		return nil, false
	}
	if p == nil {
		return nil, true
	}

	return *(*interface{})(p), true
}

// tryLoadAndStore atomically loads and stores a value if the entry is not
// expunged and its old value == @old.
//
// If the entry is expunged or its value != old, tryCompareAndSwap leaves the entry unchanged and
// returns false.
func (e *entry) tryCompareAndSwap(oldValue, newValue interface{}) (interface{}, bool) {
	p := atomic.LoadPointer(&e.p)
	if p == expunged {
		return nil, false
	}

	// Copy the interface after the first load to make this method more amenable
	// to escape analysis: if we hit the "load" path or the entry is expunged, we
	// shouldn't bother heap-allocating.
	value := newValue
	for {
		if atomic.CompareAndSwapPointer(&e.p, unsafe.Pointer(&oldValue), unsafe.Pointer(&value)) {
			return oldValue, true
		}
		v, ok := e.loadWithNil()
		if !ok {
			return nil, false
		}
		if v != oldValue {
			return v, false
		}
	}
}

// CompareAndSwap returns the existing value for the key if present.
// If the key existed and its value == @oldValue, the loaded result is true and its value is updated by @newValue.
// Please Attention that the oldValue shoule be the same(Value and Address) as the @value use in function Store.
// Cause we compare by CompareAndSwapPointer.
//
// 这里需要注意的是，oldValue一定要是Store存储的时候使用的原原本本的原value，
// 否则就算值相等也无济于事，因为上面使用的是CompareAndSwapPointer
func (m *Map) CompareAndSwap(key, oldValue, newValue interface{}) (actual interface{}, loaded bool) {
	// Avoid locking if it's a clean hit.
	read, _ := m.read.Load().(readOnly)
	if e, ok := read.m[key]; ok {
		actual, loaded = e.tryCompareAndSwap(oldValue, newValue)
		if loaded {
			atomic.AddInt64(&(m.size), 1)
		}
		return
	}

	m.mu.Lock()
	read, _ = m.read.Load().(readOnly)
	// it existed in read
	if e, ok := read.m[key]; ok {
		actual, loaded = e.tryCompareAndSwap(oldValue, newValue)
	} else if e, ok := m.dirty[key]; ok {
		// it exist in dirty but does not exist in read
		actual, loaded = e.tryCompareAndSwap(oldValue, newValue)
		m.missLocked()
	}
	// else
	// read 和 dirty中都不存在

	m.mu.Unlock()

	if loaded {
		atomic.AddInt64(&(m.size), 1)
	}

	return
}

func (e *entry) delete() (hadValue bool) {
	for {
		p := atomic.LoadPointer(&e.p)
		if p == nil || p == expunged {
			return false
		}
		if atomic.CompareAndSwapPointer(&e.p, p, nil) {
			return true
		}
	}
}

// Delete deletes the value for a key.
func (m *Map) Delete(key interface{}) {
	read, _ := m.read.Load().(readOnly)
	e, ok := read.m[key]
	if !ok && read.amended {
		// read中不存在相应值，dirty中存在
		m.mu.Lock()
		read, _ = m.read.Load().(readOnly)
		e, ok = read.m[key]
		if !ok && read.amended {
			// 再次校验后read中不存在相应值，dirty中存在
			delete(m.dirty, key)
			atomic.AddInt64(&(m.size), -1)
		}
		m.mu.Unlock()
	}
	if ok {
		// read和dirty中都存在相应值，直接置为nil，
		// 在read向dirty复制(dirtyLocked)时，这个字段会被置为expunged
		if e.delete() {
			atomic.AddInt64(&(m.size), -1)
		}
	}
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
//
// Range does not necessarily correspond to any consistent snapshot of the Map's
// contents: no key will be visited more than once, but if the value for any key
// is stored or deleted concurrently, Range may reflect any mapping for that key
// from any point during the Range call.
//
// Range may be O(N) with the number of elements in the map even if f returns
// false after a constant number of calls.
func (m *Map) Range(f func(key, value interface{}) bool) {
	read, _ := m.read.Load().(readOnly)
	if read.amended {
		// dirty存在，则把dirty提升为read
		m.mu.Lock()
		// 双检查
		read, _ = m.read.Load().(readOnly)
		if read.amended {
			read = readOnly{m: m.dirty}
			m.read.Store(read)
			m.dirty = nil
			m.misses = 0
		}
		m.mu.Unlock()
	}

	// 遍历read
	for k, e := range read.m {
		v, ok := e.load()
		if !ok {
			continue
		}
		if !f(k, v) {
			break
		}
	}
}

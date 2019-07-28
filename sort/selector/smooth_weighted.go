package gxselector

import (
	"sync"
)

// ref: https://github.com/smallnest/weighted/blob/master/smooth_weighted.go
// ref: https://mozillazg.com/2019/02/load-balancing-strategy-algorithm-weighted-round-robin.html
// ref: https://colobu.com/2016/12/04/smooth-weighted-round-robin-algorithm/

// todo
// ref: https://www.infoq.cn/article/SEbuh0K6jI*yTfqzcihB
// key point 1: 在权重值高的前三个节点中随机选取一个，以避免 "导致已感知到后端机器权重变化的接入层 Tengine 都会将第一个请求转发到权重被调高的机器上"，
// "权重被调高机器的 QPS 瞬间暴涨"；
// 实际操练过程中，避免瞬间把某个服务的权重突然调整的太高，应该缓慢增加
// 2：把所有权值计算为一个数组存储下来，用空间换取时间，加快计算速度；
// "SWRR 算法的选取机器的时间复杂度为 O(N) (其中 N 代表后端机器数量)，这就相当于每个请求都要执行接近 2000 次循环才能找到对应本次转发的后端机器"

type SWItem interface {
	Index() int        // its index in all items
	Item() interface{} // ite primary item
}

// weightedItem is a wrapped weighted item.
/*
每个节点有三个属性，这三个属性的含义如下：

weight: 节点权重值，这个值固定不变。
effective_weight: 节点的有效权重。初始值等于 weight 。之所以有个 effective_weight 是为了 在发现后端节点异常次数过多时（比如响应完成时发现选择的节点有问题，错误次数有点多）临时降低节点的权重。 在轮询遍历选择节点时这个 effective_weight 的值会逐步增加恢复到 weight 的值，所以只是为了在异常时临时降低权重不会永久影响节点的权重（节点正常时会逐步恢复到原有的权重）。
current_weight: 节点当前权重。初始值为 0，后面会动态调整：
每次选取节点时，遍历可用节点，遍历时把当前节点的 current_weight 的值加上它的 effective_weight ，
同时累加所有节点的 effective_weight 值为 total 。
如果当前节点的 current_weight 值最大，那么这个节点就是被选中的节点，同时把它的 current_weight 减去 total
没有被选中的节点的 current_weight 不用减少。
*/

type weightedItem struct {
	item            interface{}
	index           int
	Weight          int
	CurrentWeight   int // current weight is 0
	EffectiveWeight int // 主要用于请求失败次数过多时，临时调节权重
	FailedNumber    int
}

func (wi *weightedItem) Index() int {
	if wi == nil {
		return -1
	}

	return wi.index
}

func (wi *weightedItem) Item() interface{} {
	return wi.item
}

/*
SW (Smooth Weighted) is a struct that contains weighted items and provides methods to select a weighted item.
It is used for the smooth weighted round-robin balancing algorithm. This algorithm is implemented in Nginx:
https://github.com/phusion/nginx/commit/27e94984486058d73157038f7950a0a36ecc6e35.
Algorithm is as follows: on each peer selection we increase current_weight
of each eligible peer by its weight, select peer with greatest current_weight
and reduce its current_weight by total number of weight points distributed
among peers.
In case of { 5, 1, 1 } weights this gives the following sequence of
current_weight's: (a, a, b, a, c, a, a)
*/

const (
	MaxFails = 10
)

type SW struct {
	n           int
	totalWeight int
	maxFails    int
	lock        sync.Mutex
	items       []*weightedItem
}

func NewSW(maxFails int) *SW {
	sw := &SW{
		maxFails: MaxFails,
		items:    make([]*weightedItem, 0, 32),
	}

	if maxFails > 0 {
		sw.maxFails = maxFails
	}

	return sw
}

func (w *SW) Release(item SWItem, success bool) {
	index := item.Index()
	if index < 0 {
		return
	}

	w.lock.Lock()
	defer w.lock.Unlock()

	if w.n <= index {
		return
	}

	it := w.items[index]
	if success {
		it.FailedNumber = 0
	} else {
		it.FailedNumber++
		if it.FailedNumber > w.maxFails {
			it.EffectiveWeight -= it.Weight/w.maxFails + 1
		}
	}

	//w.EffectiveWeight -= w.Weight
	if it.EffectiveWeight < 0 {
		it.EffectiveWeight = 0
	}
}

// Add a weighted server.
func (w *SW) Add(item interface{}, weight int) {
	weighted := &weightedItem{item: item, Weight: weight, EffectiveWeight: weight}

	w.lock.Lock()
	defer w.lock.Unlock()

	weighted.index = w.n
	w.items = append(w.items, weighted)
	w.n++
	w.totalWeight += weight
}

// RemoveAll removes all weighted items.
func (w *SW) RemoveAll() {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.items = w.items[:0]
	w.n = 0
	w.totalWeight = 0
}

//Reset resets all current weights.
func (w *SW) Reset() {
	w.lock.Lock()
	defer w.lock.Unlock()

	for _, s := range w.items {
		s.EffectiveWeight = s.Weight
		s.CurrentWeight = 0
	}
}

// All returns all items.
func (w *SW) All() map[interface{}]int {
	w.lock.Lock()
	defer w.lock.Unlock()

	m := make(map[interface{}]int)
	for _, i := range w.items {
		m[i.Item] = i.Weight
	}
	return m
}

// Next returns next selected server.
func (w *SW) Next() SWItem {
	w.lock.Lock()
	defer w.lock.Unlock()

	return w.nextWeighted()
}

// nextWeighted returns next selected weighted object.
func (w *SW) nextWeighted() *weightedItem {
	if w.n == 0 {
		return nil
	}
	if w.n == 1 {
		return w.items[0]
	}

	return nextSmoothWeighted(w.items, w.totalWeight)
}

//https://github.com/phusion/nginx/commit/27e94984486058d73157038f7950a0a36ecc6e35
func nextSmoothWeighted(items []*weightedItem, total int) (best *weightedItem) {
	//total := 0

	for i := 0; i < len(items); i++ {
		w := items[i]

		if w == nil {
			continue
		}

		w.CurrentWeight += w.EffectiveWeight
		//total += w.EffectiveWeight
		if w.EffectiveWeight < w.Weight {
			w.EffectiveWeight++
		}

		if best == nil || w.CurrentWeight > best.CurrentWeight {
			best = w
		}

	}

	if best == nil {
		return nil
	}

	best.CurrentWeight -= total
	return best
}

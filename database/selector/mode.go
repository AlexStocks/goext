/******************************************************
# DESC    : selector mode
# AUTHOR  : Alex Stocks
# VERSION : 1.0
# LICENCE : Apache Licence 2.0
# EMAIL   : alexstocks@foxmail.com
# MOD     : 2016-07-27 17:39
# FILE    : mode.go
******************************************************/

package selector

import (
	"math/rand"
	"sync"
	"time"
)

import (
	"github.com/AlexStocks/dubbogo/registry"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type SelectorModeFunc func([]*registry.ServiceURL) Next

// Random is a random strategy algorithm for node selection
func random(services []*registry.ServiceURL) Next {
	return func(ID uint64) (*registry.ServiceURL, error) {
		if len(services) == 0 {
			return nil, ErrNoneAvailable
		}

		i := ((uint64)(rand.Int()) + ID) % (uint64)(len(services))
		return services[i], nil
	}
}

// RoundRobin is a roundrobin strategy algorithm for node selection
func roundRobin(services []*registry.ServiceURL) Next {
	var i uint64
	var mtx sync.Mutex

	return func(ID uint64) (*registry.ServiceURL, error) {
		if len(services) == 0 {
			return nil, ErrNoneAvailable
		}

		mtx.Lock()
		node := services[(ID+i)%(uint64)(len(services))]
		i++
		mtx.Unlock()

		return node, nil
	}
}

//////////////////////////////////////////
// selector mode
//////////////////////////////////////////

/*
RandomLoadBalance
1. RandomLoadBalance: 随机访问策略，按权重设置随机概率，是默认策略
1）获取所有 invokers 的个数
2）遍历所有 Invokers, 获取计算每个 invokers 的权重，并把权重累计加起来 每相邻的两个 invoker 比较他们的权重是否一样，有一个不一样说明权重不均等
3）总权重大于零且权重不均等的情况下 按总权重获取随机数 offset = random.netx(totalWeight); 遍历 invokers 确定随机数 offset 落在哪个片段(invoker 上)
4）权重相同或者总权重为 0， 根据 invokers 个数均等选择 invokers.get(random.nextInt(length))

RoundRobinLoadBalance:轮询，按公约后的权重设置轮询比率
1）获取轮询 key  服务名+方法名 获取可供调用的 invokers 个数 length 设置最大权重的默认值 maxWeight=0 设置最小权重的默认值 minWeight=Integer.MAX_VALUE
2）遍历所有 Inokers，比较出得出 maxWeight 和 minWeight
3）如果权重是不一样的 根据 key 获取自增序列 自增序列加一与最大权重取模默认得到 currentWeigth 遍历所有 invokers 筛选出大于 currentWeight 的 invokers 设置可供调用的 invokers 的个数 length
4）自增序列加一并与 length 取模，从 invokers 获取 invoker

LeastActiveLoadBalance: 最少活跃调用数， 相同的活跃的随机选择， 活跃数是指调用前后的计数差， 使慢的提供者收到更少的请求，因为越慢的提供者前 后的计数差越大。 活跃计数的功能消费者是在 ActiveLimitFilter 中设置的
 最少活跃的选择过程如下：
1）获取可调用 invoker 的总个数 初始化最小活跃数，相同最小活跃的个数 相同最小活跃数的下标数组 等等
2）遍历所有 invokers， 获取每个 invoker 的获取数 active 和权重 找出最小权重的 invoker 如果有相同最小权重的 inovkers， 将下标记录到数组 leastIndexs[]数组中 累计所有的权重到 totalWeight 变量
3）如果 invokers 的权重不相等且 totalWeight 大于 0 按总权重随机 offsetWeight = random.nextInt(totalWeight) 计算随机值在哪个片段上并返回 invoker
4）如果 invokers 的权重相等或者 totalWeight 等于 0，均等随机

ConsistentHashLoadBalance
ConsistentHashLoadBalance:一致性 hash, 相同参数的请求总是发到同一个提供者，当某一台 提供者挂时，原本发往该提供者的请求，基于虚拟节点，平摊到其它提供者，不会引起剧烈 变 动 。 对 于 一 致 性 哈 希 算 法 介 绍 网 上 很 多 ， 这 个 给 出 一 篇 http://blog.csdn.net/sparkliang/article/details/5279393 供 参 考 ， 读 者 请 自 行 阅 读 ConsistentashLoadBalance 中对一致性哈希算法的实现，还是比较通俗易懂的这里不再啰嗦。
*/

// SelectorMode defines the algorithm of selecting a provider from cluster
type SelectorMode int

const (
	SM_BEGIN SelectorMode = iota
	SM_Random
	SM_RoundRobin
	SM_LeastActive
	SM_ConsistentHash
	SM_END
)

var selectorModeStrings = [...]string{
	"Begin",
	"Random",
	"RoundRobin",
	"LeastActive",
	"ConsistentHash",
	"End",
}

func (s SelectorMode) String() string {
	if SM_BEGIN < s && s < SM_END {
		return selectorModeStrings[s]
	}

	return ""
}

var (
	selectorModeFuncs = []SelectorModeFunc{
		SM_BEGIN:          random,
		SM_Random:         random,
		SM_RoundRobin:     roundRobin,
		SM_LeastActive:    random,
		SM_ConsistentHash: random,
		SM_END:            random,
	}
)

func SelectorNext(mode SelectorMode) SelectorModeFunc {
	if mode < SM_BEGIN || SM_END < mode {
		mode = SM_Random
	}

	return selectorModeFuncs[mode]
}

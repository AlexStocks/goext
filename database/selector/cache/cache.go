package cache

import (
	"sync"
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
)

import (
	"github.com/AlexStocks/dubbogo/common"
	"github.com/AlexStocks/dubbogo/registry"
	"github.com/AlexStocks/dubbogo/selector"
)

const (
	SERVICE_WEIGHT = 10
)

var (
	// selector每DefaultTTL分钟通过tick函数清空cache或者get函数去清空某个service的cache，
	// 以全量获取某个service的所有providers
	DefaultTTL = 5 * time.Minute
)

/*
	Cache selector is a selector which uses the registry.Watcher to Cache service entries.
	It defaults to a TTL for DefaultTTL and causes a cache miss on the next request.
*/
type cacheSelector struct {
	so  selector.Options // registry & strategy
	ttl time.Duration

	// registry cache
	wg sync.WaitGroup
	sync.Mutex
	cache map[string][]*registry.ServiceURL
	ttls  map[string]time.Time // 每个数组的创建时间

	// used to close or reload watcher
	reload chan bool
	exit   chan bool
}

func (c *cacheSelector) quit() bool {
	select {
	case <-c.exit:
		return true
	default:
		return false
	}
}

// cp copies a service. Because we're caching handing back pointers would
// create a race condition, so we do this instead its fast enough
// 这个函数会被下面的get函数调用，get调用期间会启用lock
func (c *cacheSelector) cp(current []*registry.ServiceURL) []*registry.ServiceURL {
	var services []*registry.ServiceURL

	for _, service := range current {
		services = append(services, service)
	}

	return services
}

// 此接口无用
func (c *cacheSelector) del(service string) {
	delete(c.cache, service)
	delete(c.ttls, service)
}

func (c *cacheSelector) get(service string) ([]*registry.ServiceURL, error) {
	c.Lock()
	defer c.Unlock()

	// check the cache first
	services, ok := c.cache[service]
	ttl, kk := c.ttls[service]
	log.Debug("c.cache[service{%v}] = services{%v}", service, services)

	// got results, copy and return
	if ok && len(services) > 0 {
		// only return if its less than the ttl
		// 拷贝services的内容，防止发生add/del event时影响results内容
		if kk && time.Since(ttl) < c.ttl {
			return c.cp(services), nil
		}
		log.Warn("c.cache[service{%v}] = services{%v}, array ttl{%v} is less than cache.ttl{%v}",
			service, services, ttl, c.ttl)
	}

	// cache miss or ttl expired
	// now ask the registry
	ss, err := c.so.Registry.GetService(service)
	if err != nil {
		log.Error("registry.GetService(service{%v}) = err{%T, %v}", service, err, err)
		if ok && len(services) > 0 {
			log.Error("service{%v} timeout. can not get new service array, use old instead", service)
			return services, nil // 超时后，如果获取不到新的，就先暂用旧的
		}
		return nil, err
	}

	// we didn't have any results so cache
	c.cache[service] = c.cp(ss)
	c.ttls[service] = time.Now().Add(c.ttl)
	return ss, nil
}

// update函数调用set函数，update函数调用期间会启用lock
func (c *cacheSelector) set(service string, services []*registry.ServiceURL) {
	if 0 < len(services) {
		c.cache[service] = services
		c.ttls[service] = time.Now().Add(c.ttl)

		return
	}

	delete(c.cache, service)
	delete(c.ttls, service)
}

func ArrayRemoveAt(a interface{}, i int) {
	if i < 0 {
		return
	}

	if array, ok := a.(*[]*registry.ServiceURL); ok {
		if len(*array) <= i {
			return
		}
		s := *array
		s = append(s[:i], s[i+1:]...)
		*array = s
	}
}

func (c *cacheSelector) update(res *registry.Result) {
	if res == nil || res.Service == nil {
		return
	}
	var (
		ok       bool
		services []*registry.ServiceURL
		sname    string
	)

	log.Debug("update @registry result{%s}", res)
	sname = res.Service.Query.Get("interface")
	c.Lock()
	services, ok = c.cache[sname]
	log.Debug("service name:%s, get service{%#v} event, its current member lists:", sname, services)
	if ok { // existing service found
		for i, s := range services {
			log.Debug("cache.services[%s][%d] = service{%#v}", sname, i, s)
			if s.PrimitiveURL == res.Service.PrimitiveURL {
				ArrayRemoveAt(&(services), i)
			}
		}
	}

	switch res.Action {
	case registry.ServiceURLAdd, registry.ServiceURLUpdate:
		services = append(services, res.Service)
		log.Info("selector add serviceURL{%#v}", *res.Service)
	case registry.ServiceURLDel:
		log.Error("selector delete serviceURL{%#v}", *res.Service)
	}
	c.set(sname, services)
	services, ok = c.cache[sname]
	log.Debug("after update, cache.services[%s] member list size{%d}", sname, len(services))
	// if ok { // debug
	// 	for i, s := range services {
	// 		log.Debug("cache.services[%s][%d] = service{%#v}", sname, i, s)
	// 	}
	// }
	c.Unlock()
}

// run starts the cache watcher loop
// it creates a new watcher if there's a problem
// reloads the watcher if Init is called
// and returns when Close is called
func (c *cacheSelector) run() {
	defer c.wg.Done()
	// 这个函数会周期性地清空cache，从函数注释来看作者对这个函数设计也存疑，而如此频繁的刷新会导致register不断地在zk上创建大量临时节点，故而我觉得不用它比较好 // AlexStocks 2016/07/09
	// 其实，Select调用get函数获取service的service url array的时候，也会进行超时检查
	//
	// 将来进行负载均衡开发的时候，可以重新打开这个函数，进行权重更新计算
	// go c.tick() // 这个tick是周期性执行函数，周期性的清空cache

	// 除非收到quit信号，否则一直卡在watch上，watch内部也是一个loop
	for {
		// exit early if already dead
		if c.quit() {
			log.Warn("(cacheSelector)run() quit now")
			return
		}

		// create new watcher
		// 创建新watch，走到这一步要么第一次for循环进来，要么是watch函数出错。watch出错说明registry.watch的zk client与zk连接出现了问题
		w, err := c.so.Registry.Watch()
		log.Debug("cache.Registry.Watch() = watch{%#v}, error{%#v}", w, err)
		if err != nil {
			// log.Println(err)
			log.Warn("cacheSelector.Registry.Watch() = error{%v}", err)
			time.Sleep(common.TimeSecondDuration(registry.REGISTRY_CONN_DELAY))
			continue
		}

		// watch for events
		// 除非watch遇到错误，否则watch函数内部的for循环一直运行下午，run函数的for循环也会一直卡在watch函数上
		// watch一旦退出，就会执行registry.Watch().Stop, 相应的client就会无效
		err = c.watch(w)
		log.Debug("cache.watch(w) = err{%#v}", err)
		if err != nil {
			// log.Println(err)
			log.Warn("cacheSelector.watch() = error{%v}", err)
			time.Sleep(common.TimeSecondDuration(registry.REGISTRY_CONN_DELAY))
			continue
		}
	}
}

// check cache and expire on each tick
// tick函数每隔一分钟把cacheSelector.cache中所有内容clear一次，这样做貌似不好
func (c *cacheSelector) tick() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			c.Lock()
			for service, expiry := range c.ttls {
				if d := time.Since(expiry); d > c.ttl {
					// TODO: maybe refresh the cache rather than blowing it away
					log.Warn("tick delete service{%s}", service)
					c.del(service)
				}
			}
			c.Unlock()
		case <-c.exit:
			return
		}
	}
}

// watch loops the next event and calls update
// it returns if there's an error
// 如果收到了退出信号或者Next调用返回错误，就调用watcher的stop函数
func (c *cacheSelector) watch(w registry.Watcher) error {
	var (
		err  error
		res  *registry.Result
		exit chan struct{}
	)
	// manage this loop
	exit = make(chan struct{})

	defer func() {
		close(exit)
		w.Stop()
	}()
	c.wg.Add(1)
	go func() {
		// wait for exit or reload signal
		// 上面的意思是这个goroutine会一直卡在这个select段上，直到收到exit或者reload signal
		select {
		case <-c.exit:
			w.Stop() // stop之后下面的Next函数就会返回error
		case <-c.reload: // 除非Init函数被调用，否则reload不会收到任何信号
			// stop the watcher
			w.Stop()
		case <-exit:
		}
		c.wg.Done()
	}()

	for {
		res, err = w.Next()
		log.Debug("watch.Next() = result{%s}, error{%#v}", res, err)
		if err != nil {
			return err
		}
		if res.Action == registry.ServiceURLDel && !w.Valid() {
			// consumer与registry连接中断的情况下，为了服务的稳定不删除任何provider
			log.Warn("update @result{%s}. But its connection to registry is invalid", res)
			continue
		}

		c.update(res)
	}
}

func (c *cacheSelector) Init(opts ...selector.Option) error {
	for _, o := range opts {
		o(&c.so)
	}

	// reload the watcher
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		select {
		case <-c.exit:
			return
		default:
			c.reload <- true
		}
	}()

	return nil
}

func (c *cacheSelector) Options() selector.Options {
	return c.so
}

func (c *cacheSelector) Select(service string) (selector.Next, error) {
	var (
		err      error
		services []*registry.ServiceURL
	)

	// get the service
	// try the cache first
	// if that fails go directly to the registry
	services, err = c.get(service)
	log.Debug("get(service{%s} = serviceURL array{%#v})", service, services)
	if err != nil {
		log.Error("cache.get(service{%s}) = error{%T-%v}", service, err, err)
		// return nil, err
		return nil, selector.ErrNotFound
	}
	for i, s := range services {
		log.Debug("services[%d] = serviceURL{%#v}", i, s)
	}
	// if there's nothing left, return
	if len(services) == 0 {
		return nil, selector.ErrNoneAvailable
	}

	return selector.SelectorNext(c.so.Mode)(services), nil
}

func (c *cacheSelector) Mark(service string, serviceURL *registry.ServiceURL, err error) {
	return
}

func (c *cacheSelector) Reset(service string) {
	return
}

// Close stops the watcher and destroys the cache
// Close函数清空service url cache，且发出exit signal以停止watch的运行
func (c *cacheSelector) Close() error {
	c.Lock()
	c.cache = make(map[string][]*registry.ServiceURL)
	c.Unlock()

	select {
	case <-c.exit: // 这里这个技巧非常好，检测是否已经close了c.exit
		return nil
	default:
		close(c.exit)
	}
	c.wg.Wait()
	return nil
}

func (c *cacheSelector) String() string {
	return "cache selector"
}

// selector主要有两个接口，对外接口Select用于获取地址，select调用get，get调用cp;
// 对内接口run调用watch,watch则调用update，update调用set，以用于接收add/del service url.
//
// 还有两个接口Init和Close,Init用于发出reload信号，重新初始化selector，而Close则是发出stop信号，停掉watch，清算破产
//
// registor自身主要向selector暴露了watch功能
func NewSelector(opts ...selector.Option) selector.Selector {
	sopts := selector.Options{
		Mode: selector.SM_Random,
	}

	for _, opt := range opts {
		opt(&sopts)
	}

	if sopts.Registry == nil {
		panic("@opts.Registry is nil")
	}

	ttl := DefaultTTL

	if sopts.Context != nil {
		if t, ok := sopts.Context.Value(common.DUBBOGO_CTX_KEY).(time.Duration); ok {
			ttl = t
		}
	}

	c := &cacheSelector{
		so:     sopts,
		ttl:    ttl,
		cache:  make(map[string][]*registry.ServiceURL),
		ttls:   make(map[string]time.Time),
		reload: make(chan bool, 1),
		exit:   make(chan bool),
	}

	c.wg.Add(1)
	go c.run()
	return c
}

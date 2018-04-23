// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxregistry provides a interface for service selector
package gxcache

import (
	"sync"
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
)

import (
	"github.com/AlexStocks/dubbogo/common"
	"github.com/AlexStocks/goext/database/registry"
	"github.com/AlexStocks/goext/database/selector"
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
type Selector struct {
	so  gxselector.Options // registry & strategy
	ttl time.Duration

	// registry cache
	wg sync.WaitGroup
	sync.Mutex
	cache map[string][]*gxregistry.Service
	ttls  map[string]time.Time // 每个数组的创建时间

	// used to close or reload watcher
	reload chan struct{}
	done   chan struct{}
}

func (s *Selector) quit() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// cp copies a service. Because we're caching handing back pointers would
// create a race condition, so we do this instead its fast enough
// 这个函数会被下面的get函数调用，get调用期间会启用lock
func (s *Selector) cp(current []*gxregistry.Service) []*gxregistry.Service {
	var services []*gxregistry.Service

	for _, service := range current {
		services = append(services, service)
	}

	return services
}

// 此接口无用
func (s *Selector) del(service string) {
	delete(s.cache, service)
	delete(s.ttls, service)
}

func (s *Selector) get(attr gxregistry.ServiceAttr) ([]*gxregistry.Service, error) {
	s.Lock()
	defer s.Unlock()

	serviceString := attr.Service
	// check the cache first
	services, ok := s.cache[serviceString]
	ttl, kk := s.ttls[serviceString]
	log.Debug("s.cache[serviceString{%v}] = services{%v}", serviceString, services)

	// got results, copy and return
	if ok && len(services) > 0 {
		// only return if its less than the ttl
		// 拷贝services的内容，防止发生add/del event时影响results内容
		if kk && time.Since(ttl) < s.ttl {
			return s.cp(services), nil
		}
		log.Warn("s.cache[serviceString{%v}] = services{%v}, array ttl{%v} is less than cache.ttl{%v}",
			serviceString, services, ttl, s.ttl)
	}

	// cache miss or ttl expired
	// now ask the registry
	svc, err := s.so.Registry.GetService(attr)
	if err != nil {
		log.Error("registry.GetService(serviceString{%v}) = err{%T, %v}", serviceString, err, err)
		if ok && len(services) > 0 {
			log.Error("serviceString{%v} timeout. can not get new serviceString array, use old instead", serviceString)
			return services, nil // 超时后，如果获取不到新的，就先暂用旧的
		}
		return nil, err
	}

	var serviceArray []*gxregistry.Service
	for _, node := range svc.Nodes {
		serviceArray = append(serviceArray, &gxregistry.Service{Attr: svc.Attr, Nodes: []*gxregistry.Node{node}})
	}

	// we didn't have any results so cache
	s.cache[serviceString] = s.cp(serviceArray)
	s.ttls[serviceString] = time.Now().Add(s.ttl)

	return serviceArray, nil
}

// update函数调用set函数，update函数调用期间会启用lock
func (s *Selector) set(service string, services []*gxregistry.Service) {
	if 0 < len(services) {
		s.cache[service] = services
		s.ttls[service] = time.Now().Add(s.ttl)

		return
	}

	delete(s.cache, service)
	delete(s.ttls, service)
}

func filterServices(array *[]*gxregistry.Service, i int) {
	if i < 0 {
		return
	}

	if len(*array) <= i {
		return
	}
	s := *array
	s = append(s[:i], s[i+1:]...)
	*array = s
}

func (s *Selector) update(res *gxregistry.EventResult) {
	if res == nil || res.Service == nil {
		return
	}
	var (
		ok       bool
		services []*gxregistry.Service
		sname    string
	)

	log.Debug("update @registry result{%s}", res)
	sname = res.Service.Attr.Service
	s.Lock()
	services, ok = s.cache[sname]
	log.Debug("service name:%s, get service{%#v} event, its current member lists:", sname, services)
	if ok { // existing service found
		for i, s := range services {
			log.Debug("cache.services[%s][%d] = service{%#v}", sname, i, s)
			if s.Equal(res.Service) {
				filterServices(&(services), i)
			}
		}
	}

	switch res.Action {
	case gxregistry.ServiceAdd, gxregistry.ServiceUpdate:
		services = append(services, res.Service)
		log.Info("selector add serviceURL{%#v}", *res.Service)
	case gxregistry.ServiceDel:
		log.Error("selector delete serviceURL{%#v}", *res.Service)
	}
	s.set(sname, services)
	services, ok = s.cache[sname]
	log.Debug("after update, cache.services[%s] member list size{%d}", sname, len(services))
	// if ok { // debug
	// 	for i, s := range services {
	// 		log.Debug("cache.services[%s][%d] = service{%#v}", sname, i, s)
	// 	}
	// }
	s.Unlock()
}

// run starts the cache watcher loop
// it creates a new watcher if there's a problem
// reloads the watcher if Init is called
// and returns when Close is called
func (s *Selector) run() {
	defer s.wg.Done()
	// 这个函数会周期性地清空cache，从函数注释来看作者对这个函数设计也存疑，而如此频繁的刷新会导致register不断地在zk上创建大量临时节点，
	// 故而我觉得不用它比较好 // AlexStocks 2016/07/09
	//
	// 其实，Select调用get函数获取service的service url array的时候，也会进行超时检查
	//
	// 将来进行负载均衡开发的时候，可以重新打开这个函数，进行权重更新计算
	// go s.tick() // 这个tick是周期性执行函数，周期性的清空cache

	// 除非收到quit信号，否则一直卡在watch上，watch内部也是一个loop
	for {
		// done early if already dead
		if s.quit() {
			log.Warn("(Selector)run() quit now")
			return
		}

		// create new watcher
		// 创建新watch，走到这一步要么第一次for循环进来，要么是watch函数出错。watch出错说明registry.watch的zk client与zk连接出现了问题
		w, err := s.so.Registry.Watch()
		log.Debug("cache.Registry.Watch() = watch{%#v}, error{%#v}", w)
		if err != nil {
			if s.quit() {
				log.Warn("(Selector)run() quit now")
				return
			}
			log.Warn("Registry.Watch() = error:%+v", err)
			continue
		}

		// watch for events
		// 除非watch遇到错误，否则watch函数内部的for循环一直运行下午，run函数的for循环也会一直卡在watch函数上
		// watch一旦退出，就会执行registry.Watch().Stop, 相应的client就会无效
		err = s.watch(w)
		log.Debug("cache.watch(w) = err{%#v}", err)
		if err != nil {
			// log.Println(err)
			log.Warn("Selector.watch() = error{%v}", err)
			time.Sleep(common.TimeSecondDuration(gxregistry.REGISTRY_CONN_DELAY))
			continue
		}
	}
}

// check cache and expire on each tick
// tick函数每隔一分钟把Selector.cache中所有内容clear一次，这样做貌似不好
func (s *Selector) tick() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			s.Lock()
			for service, expiry := range s.ttls {
				if d := time.Since(expiry); d > s.ttl {
					// TODO: maybe refresh the cache rather than blowing it away
					log.Warn("tick delete service{%s}", service)
					s.del(service)
				}
			}
			s.Unlock()
		case <-s.done:
			return
		}
	}
}

// watch loops the next event and calls update
// it returns if there's an error
// 如果收到了退出信号或者Next调用返回错误，就调用watcher的stop函数
func (s *Selector) watch(w gxregistry.Watcher) error {
	var (
		err  error
		res  *gxregistry.EventResult
		done chan struct{}
	)
	// manage this loop
	done = make(chan struct{})

	defer func() {
		close(done)
		w.Stop()
	}()
	s.wg.Add(1)
	go func() {
		// wait for done or reload signal
		// 上面的意思是这个goroutine会一直卡在这个select段上，直到收到done或者reload signal
		select {
		case <-s.done:
			w.Stop() // stop之后下面的Next函数就会返回error
		case <-s.reload: // 除非Init函数被调用，否则reload不会收到任何信号
			// stop the watcher
			w.Stop()
		case <-done:
		}
		s.wg.Done()
	}()

	for {
		res, err = w.Notify()
		log.Debug("watch.Notify() = result{%s}, error{%#v}", res, err)
		if err != nil {
			return err
		}
		if res.Action == gxregistry.ServiceDel && !w.Valid() {
			// consumer与registry连接中断的情况下，为了服务的稳定不删除任何provider
			log.Warn("update @result{%s}. But its connection to registry is invalid", res)
			continue
		}

		s.update(res)
	}
}

func (s *Selector) Init(opts ...gxselector.Option) error {
	for _, o := range opts {
		o(&s.so)
	}

	// reload the watcher
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case <-s.done:
			return
		default:
			s.reload <- struct{}{}
		}
	}()

	return nil
}

func (s *Selector) Options() gxselector.Options {
	return s.so
}

func (s *Selector) Select(service gxregistry.ServiceAttr) (gxselector.Next, error) {
	var (
		err      error
		services []*gxregistry.Service
	)

	// get the service
	// try the cache first
	// if that fails go directly to the registry
	services, err = s.get(service)
	log.Debug("get(service{%+v} = serviceURL array{%+v})", service, services)
	if err != nil {
		log.Error("cache.get(service{%s}) = error{%T-%v}", service, err, err)
		// return nil, err
		return nil, gxselector.ErrNotFound
	}
	//for i, s := range services {
	//	log.Debug("services[%d] = serviceURL{%#v}", i, s)
	//}
	// if there's nothing left, return
	if len(services) == 0 {
		return nil, gxselector.ErrNoneAvailable
	}

	return gxselector.SelectorNext(s.so.Mode)(services), nil
}

// Close stops the watcher and destroys the cache
// Close函数清空service url cache，且发出done signal以停止watch的运行
func (s *Selector) Close() error {
	s.Lock()
	s.cache = make(map[string][]*gxregistry.Service)
	s.Unlock()

	select {
	case <-s.done: // 这里这个技巧非常好，检测是否已经close了c.done
		return nil
	default:
		close(s.done)
	}
	s.wg.Wait()
	return nil
}

func (s *Selector) String() string {
	return "cache selector"
}

// selector主要有两个接口，对外接口Select用于获取地址，select调用get，get调用cp;
// 对内接口run调用watch,watch则调用update，update调用set，以用于接收add/del service url.
//
// 还有两个接口Init和Close,Init用于发出reload信号，重新初始化selector，而Close则是发出stop信号，停掉watch，清算破产
//
// registor自身主要向selector暴露了watch功能
func NewSelector(opts ...gxselector.Option) gxselector.Selector {
	sopts := gxselector.Options{
		Mode: gxselector.SM_Random,
	}

	for _, opt := range opts {
		opt(&sopts)
	}

	if sopts.Registry == nil {
		panic("@opts.Registry is nil")
	}

	ttl := DefaultTTL

	if sopts.Context != nil {
		if t, ok := sopts.Context.Value(GxselectorDefaultKey).(time.Duration); ok {
			ttl = t
		}
	}

	s := &Selector{
		so:     sopts,
		ttl:    ttl,
		cache:  make(map[string][]*gxregistry.Service),
		ttls:   make(map[string]time.Time),
		reload: make(chan struct{}, 1),
		done:   make(chan struct{}),
	}

	s.wg.Add(1)
	go s.run()
	return s
}

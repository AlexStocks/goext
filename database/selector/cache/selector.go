// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxcache provides a cache selector
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
	DefaultTTL = 5 * time.Minute
)

// the cached selector
type Selector struct {
	opts gxselector.Options // registry & strategy
	ttl  time.Duration

	wg sync.WaitGroup
	sync.Mutex
	cache map[string][]*gxregistry.Service
	// the creation time for every []*gxregistry.Service.
	ttls map[string]time.Time

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

// cp is invoked by function get.
func (s *Selector) cp(current []*gxregistry.Service) []*gxregistry.Service {
	var services []*gxregistry.Service

	for _, service := range current {
		services = append(services, service)
	}

	return services
}

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

	if ok && len(services) > 0 {
		// only return if its less than the ttl
		// and copy the service array in case of its results is affected by function add/del
		if kk && time.Since(ttl) < s.ttl {
			return s.cp(services), nil
		}
		log.Warn("s.cache[serviceString{%v}] = services{%v}, array ttl{%v} is less than cache.ttl{%v}",
			serviceString, services, ttl, s.ttl)
	}

	svc, err := s.opts.Registry.GetService(attr)
	if err != nil {
		log.Error("registry.GetService(serviceString{%v}) = err{%T, %v}", serviceString, err, err)
		if ok && len(services) > 0 {
			log.Error("serviceString{%v} timeout. can not get new serviceString array, use old instead", serviceString)
			// all local services expired and can not get new services from registry, just use che cached instead
			return services, nil
		}

		return nil, err
	}

	var serviceArray []*gxregistry.Service
	for _, node := range svc.Nodes {
		serviceArray = append(serviceArray, &gxregistry.Service{Attr: svc.Attr, Nodes: []*gxregistry.Node{node}})
	}

	s.cache[serviceString] = s.cp(serviceArray)
	s.ttls[serviceString] = time.Now().Add(s.ttl)

	return serviceArray, nil
}

// update will invoke set
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

func (s *Selector) run() {
	defer s.wg.Done()
	for {
		// quit asap
		if s.quit() {
			log.Warn("(Selector)run() quit now")
			return
		}

		w, err := s.opts.Registry.Watch()
		log.Debug("cache.Registry.Watch() = watch{%#v}, error{%#v}", w)
		if err != nil {
			if s.quit() {
				log.Warn("(Selector)run() quit now")
				return
			}
			log.Warn("Registry.Watch() = error:%+v", err)
			continue
		}

		err = s.watch(w)
		log.Debug("cache.watch(w) = err{%#v}", err)
		if err != nil {
			log.Warn("Selector.watch() = error{%v}", err)
			time.Sleep(common.TimeSecondDuration(gxregistry.REGISTRY_CONN_DELAY))
			continue
		}
	}
}

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
		w.Close()
	}()
	s.wg.Add(1)
	go func() {
		select {
		case <-s.done:
			w.Close()
		case <-s.reload:
			// stop the watcher
			w.Close()
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
			// do not delete any provider when consumer failed to connect the registry.
			log.Warn("update @result{%s}. But its connection to registry is invalid", res)
			continue
		}

		s.update(res)
	}
}

func (s *Selector) Init(opts ...gxselector.Option) error {
	for _, o := range opts {
		o(&s.opts)
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
	return s.opts
}

func (s *Selector) Select(service gxregistry.ServiceAttr) (gxselector.Filter, error) {
	var (
		err      error
		services []*gxregistry.Service
	)

	services, err = s.get(service)
	log.Debug("get(service{%+v} = serviceURL array{%+v})", service, services)
	if err != nil {
		log.Error("cache.get(service{%s}) = error{%T-%v}", service, err, err)
		return nil, gxselector.ErrNotFound
	}
	if len(services) == 0 {
		return nil, gxselector.ErrNoneAvailable
	}

	return gxselector.SelectorFilter(s.opts.Mode)(services), nil
}

func (s *Selector) Close() error {
	s.Lock()
	s.cache = make(map[string][]*gxregistry.Service)
	s.Unlock()

	select {
	case <-s.done:
		return nil
	default:
		close(s.done)
	}
	s.wg.Wait()
	return nil
}

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
		if t, ok := sopts.Context.Get(GxselectorDefaultKey); ok {
			ttl = t.(time.Duration)
		}
	}

	s := &Selector{
		opts:   sopts,
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

// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxpool provides a service pool filter
package gxpool

import (
	"sync"
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
)

import (
	"github.com/AlexStocks/dubbogo/common"
	"github.com/AlexStocks/goext/database/filter"
	"github.com/AlexStocks/goext/database/registry"
	"github.com/AlexStocks/goext/time"
	jerrors "github.com/juju/errors"
)

var (
	defaultTTL    = 5 * time.Minute
	activeTimeout = 2 * time.Hour
)

type Filter struct {
	// registry & strategy
	opts gxfilter.Options
	ttl  time.Duration

	wg sync.WaitGroup
	sync.Mutex
	services map[string][]*gxregistry.Service
	// the creation time for every []*gxregistry.Service.
	active map[string]time.Time

	done chan struct{}
}

func (s *Filter) isClosed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// copy is invoked by function get.
func (s *Filter) copy(current []*gxregistry.Service, by gxregistry.ServiceAttr) []*gxregistry.Service {
	var services []*gxregistry.Service

	for _, service := range current {
		service := service
		if by.Filter(*service.Attr) {
			s := *service
			s.Nodes = make([]*gxregistry.Node, 0, len(service.Nodes))
			for _, node := range service.Nodes {
				n := *node
				s.Nodes = append(s.Nodes, &n)
			}

			services = append(services, &s)
		}
	}

	return services
}

func (s *Filter) get(attr gxregistry.ServiceAttr) ([]*gxregistry.Service, gxfilter.ServiceToken, error) {
	s.Lock()
	defer s.Unlock()

	serviceString := attr.Service
	// check the services first
	services, sok := s.services[serviceString]
	ttl, tok := s.active[serviceString]
	log.Debug("s.services[serviceString{%v}] = services{%v}", serviceString, services)

	if sok && len(services) > 0 {
		// only return if its less than the ttl
		// and copy the service array in case of its results is affected by function add/del
		if tok && time.Since(ttl) < s.ttl {
			return s.copy(services, attr), ttl.UnixNano(), nil
		}
		log.Warn("s.services[serviceString{%v}] = services{%v}, array ttl{%v} is less than services.ttl{%v}",
			serviceString, services, ttl, s.ttl)
	}

	svcs, err := s.opts.Registry.GetServices(attr)
	log.Debug("Registry.GetServices(attr:%+v) = {err:%s, svcs:%+v}", attr, err, svcs)
	if err != nil {
		log.Error("registry.GetService(serviceString{%v}) = err:%+v}", serviceString, err)
		if sok && len(services) > 0 {
			log.Error("serviceString{%v} timeout. can not get new serviceString array, use old instead", serviceString)
			// all local services expired and can not get new services from registry, just use che cached instead
			return services, ttl.UnixNano(), nil
		}

		return nil, 0, err
	}

	var serviceArray []*gxregistry.Service
	for i, svc := range svcs {
		svc := svc
		serviceArray = append(serviceArray, &svc)
		log.Debug("i:%d, svc:%+v, serviceArray:%+v", i, svc, serviceArray)
	}

	filterServiceArray := s.copy(serviceArray, attr)
	s.services[serviceString] = serviceArray
	ttl = time.Now().Add(s.ttl)
	s.active[serviceString] = ttl

	return filterServiceArray, ttl.UnixNano(), nil
}

// update will invoke set
func (s *Filter) set(service string, services []*gxregistry.Service) {
	if 0 < len(services) {
		s.services[service] = services
		s.active[service] = time.Now().Add(s.ttl)

		return
	}

	delete(s.services, service)
	delete(s.active, service)
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

func (s *Filter) update(res *gxregistry.EventResult) {
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
	services, ok = s.services[sname]
	log.Debug("service name:%s, its current member lists:%+v", sname, services)
	if ok { // existing service found
		for i, s := range services {
			log.Debug("services.services[%s][%d] = service{%#v}", sname, i, s)
			if s.Equal(res.Service) {
				filterServices(&(services), i)
				log.Debug("i:%d, new services:%+v", i, services)
			}
		}
	}

	switch res.Action {
	case gxregistry.ServiceAdd, gxregistry.ServiceUpdate:
		services = append(services, res.Service)
		log.Info("filter add serviceURL{%#v}", *res.Service)
	case gxregistry.ServiceDel:
		log.Warn("filter delete serviceURL{%#v}", *res.Service)
	}
	s.set(sname, services)
	services, ok = s.services[sname]
	log.Debug("after update, services.services[%s] member list size{%d}", sname, len(services))
	// if ok { // debug
	// 	for i, s := range services {
	// 		log.Debug("services.services[%s][%d] = service{%#v}", sname, i, s)
	// 	}
	// }
	s.Unlock()
}

func (s *Filter) run() {
	defer s.wg.Done()
	for {
		// quit asap
		if s.isClosed() {
			log.Warn("(Filter)run() isClosed now")
			return
		}

		w, err := s.opts.Registry.Watch(
			gxregistry.WithWatchRoot(s.opts.Registry.Options().Root),
		)
		log.Debug("services.Registry.Watch() = watch:%+v, error:%+v", w, err)
		if err != nil {
			if s.isClosed() {
				log.Warn("(Filter)run() isClosed now")
				return
			}
			log.Warn("Registry.Watch() = error:%+v", err)
			time.Sleep(common.TimeSecondDuration(gxregistry.REGISTRY_CONN_DELAY))
			continue
		}

		// this function will block until got done signal
		err = s.watch(w)
		log.Debug("services.watch(w) = err{%#v}", err)
		if err != nil {
			log.Warn("Filter.watch() = error{%v}", err)
			time.Sleep(common.TimeSecondDuration(gxregistry.REGISTRY_CONN_DELAY))
			continue
		}
	}
}

func (s *Filter) watch(w gxregistry.Watcher) error {
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

func (s *Filter) Options() gxfilter.Options {
	return s.opts
}

func (s *Filter) GetService(service gxregistry.ServiceAttr) ([]*gxregistry.Service, error) {
	var (
		err      error
		services []*gxregistry.Service
	)

	services, _, err = s.get(service)
	log.Debug("get(service{%+v} = serviceURL array{%+v})", service, services)
	if err != nil {
		log.Error("services.get(service{%s}) = error{%T-%v}", service, err, err)
		return nil, gxfilter.ErrNotFound
	}
	if len(services) == 0 {
		return nil, gxfilter.ErrNoneAvailable
	}

	return services, nil
}

func (s *Filter) Filter(service gxregistry.ServiceAttr) (gxfilter.Balancer, gxfilter.ServiceToken, error) {
	var (
		err      error
		token    gxfilter.ServiceToken
		services []*gxregistry.Service
	)

	services, token, err = s.get(service)
	log.Debug("get(service{%+v} = serviceURL array{%+v})", service, services)
	if err != nil {
		log.Error("services.get(service{%s}) = error{%T-%v}", service, err, err)
		return nil, 0, gxfilter.ErrNotFound
	}
	if len(services) == 0 {
		return nil, 0, gxfilter.ErrNoneAvailable
	}

	return gxfilter.BalancerModeFunc(s.opts.Mode)(services), token, nil
}

func (s *Filter) CheckTokenAlive(attr gxregistry.ServiceAttr, token gxfilter.ServiceToken) bool {
	var (
		ok     bool
		active time.Time
		t      time.Time
	)

	s.Lock()
	active, ok = s.active[attr.Service]
	s.Unlock()

	if ok {
		t = gxtime.UnixNano2Time(token)
		if activeTimeout < active.Sub(t) {
			// active expire, the user should update its service array
			return false
		}
		return active.UnixNano() == token
	}

	return false
}

func (s *Filter) Close() error {
	s.Lock()
	s.services = make(map[string][]*gxregistry.Service)
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

func NewFilter(opts ...gxfilter.Option) (gxfilter.Filter, error) {
	sopts := gxfilter.Options{
		Mode: gxfilter.SM_Random,
	}

	for _, opt := range opts {
		opt(&sopts)
	}

	if sopts.Registry == nil {
		return nil, jerrors.Errorf("@opts.Registry is nil")
	}

	ttl := defaultTTL
	if sopts.Context != nil {
		if t, ok := sopts.Context.Get(GxfilterDefaultKey); ok {
			ttl = t.(time.Duration)
		}
	}

	s := &Filter{
		opts:     sopts,
		ttl:      ttl,
		services: make(map[string][]*gxregistry.Service),
		active:   make(map[string]time.Time),
		done:     make(chan struct{}),
	}

	s.wg.Add(1)
	go s.run()
	return s, nil
}

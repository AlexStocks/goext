// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxetcd provides an etcd version 3 gxregistry
// ref: https://github.com/micro/go-plugins/blob/master/registry/etcdv3/etcdv3.go
package gxetcd

import (
	"context"
	"sync"
)

import (
	etcdv3 "github.com/coreos/etcd/clientv3"
	jerrors "github.com/juju/errors"

	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	hash "github.com/mitchellh/hashstructure"
)

import (
	"github.com/AlexStocks/goext/database/etcd"
	"github.com/AlexStocks/goext/database/registry"
	log "github.com/AlexStocks/log4go"
)

type Registry struct {
	client  *gxetcd.LeaseClient
	options gxregistry.Options
	sync.Mutex
	register map[string]uint64
	leases   map[string]etcdv3.LeaseID
}

func NewRegistry(opts ...gxregistry.Option) gxregistry.Registry {
	config := etcdv3.Config{
		Endpoints: []string{"127.0.0.1:2379"},
	}

	var options gxregistry.Options
	for _, o := range opts {
		o(&options)
	}
	if options.Timeout == 0 {
		options.Timeout = gxregistry.DefaultTimeout
	}

	var addrs []string
	for _, addr := range options.Addrs {
		if len(addr) == 0 {
			continue
		}
		addrs = append(addrs, addr)
	}

	// if we got addrs then we'll update
	if len(addrs) > 0 {
		config.Endpoints = addrs
	}

	client, err := etcdv3.New(config)
	if err != nil {
		panic(jerrors.Errorf("etcdv3.New(config:%+v) = error:%s", config, err))
	}
	gxClient, err := gxetcd.NewLeaseClient(client)
	if err != nil {
		panic(jerrors.Errorf("gxetcd.NewLeaseClient() = error:%s", err))
	}

	if options.Root == "" {
		options.Root = gxregistry.DefaultServiceRoot
	}

	return &Registry{
		client:   gxClient,
		options:  options,
		register: make(map[string]uint64),
		leases:   make(map[string]etcdv3.LeaseID),
	}
}

func (r *Registry) Options() gxregistry.Options {
	return r.options
}

func (r *Registry) Register(svc interface{}, opts ...gxregistry.RegisterOption) error {
	s, ok := svc.(*gxregistry.Service)
	if !ok {
		s1, ok := svc.(gxregistry.Service)
		if !ok {
			return jerrors.Errorf("@service:%+v type is not gxregistry.Service", svc)
		}
		s = &s1
	}

	if len(s.Nodes) == 0 {
		return jerrors.Errorf("Require at least one node")
	}

	var leaseNotFound bool
	serviceKey := s.Path(r.options.Root)
	//refreshing lease if existing
	leaseID, ok := r.leases[serviceKey]
	log.Debug("name:%s, ok:%v, leaseid:%#v", serviceKey, ok, leaseID)
	if ok {
		if _, err := r.client.EtcdClient().KeepAliveOnce(context.TODO(), leaseID); err != nil {
			if err != rpctypes.ErrLeaseNotFound {
				return err
			}

			// lease not found do register
			leaseNotFound = true
		}
	}

	// create hash of service; uint64
	h, err := hash.Hash(s, nil)
	if err != nil {
		return err
	}

	// get existing hash
	r.Lock()
	v, ok := r.register[serviceKey]
	r.Unlock()

	// the service is unchanged, skip registering
	if ok && v == h && !leaseNotFound {
		return nil
	}

	service := &gxregistry.Service{Metadata: s.Metadata}
	service.ServiceAttr = s.ServiceAttr

	var options gxregistry.RegisterOptions
	for _, o := range opts {
		o(&options)
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.options.Timeout)
	defer cancel()

	var lgr *etcdv3.LeaseGrantResponse
	if options.TTL.Seconds() > 0 {
		lgr, err = r.client.EtcdClient().Grant(ctx, int64(options.TTL.Seconds()))
		if err != nil {
			return err
		}
	}

	// register every node
	for _, node := range s.Nodes {
		service.Nodes = []*gxregistry.Node{node}
		data, err := gxregistry.EncodeService(service)
		if err != nil {
			log.Warn("gxregistry.EncodeService(service:%+v) = error:%s", service, err)
			continue
		}
		if lgr != nil {
			_, err = r.client.EtcdClient().Put(
				ctx,
				service.NodePath(r.options.Root, *node),
				data,
				etcdv3.WithLease(lgr.ID),
			)
		} else {
			_, err = r.client.EtcdClient().Put(
				ctx,
				service.NodePath(r.options.Root, *node),
				data,
			)
		}
		if err != nil {
			return err
		}
	}

	r.Lock()
	// save our hash of the service
	r.register[serviceKey] = h
	// save our leaseID of the service
	if lgr != nil {
		r.leases[serviceKey] = lgr.ID
	}
	r.Unlock()

	return nil
}

func (r *Registry) Unregister(svc interface{}) error {
	s, ok := svc.(*gxregistry.Service)
	if !ok {
		s1, ok := svc.(gxregistry.Service)
		if !ok {
			return jerrors.Errorf("@service:%+v type is not gxregistry.Service", svc)
		}
		s = &s1
	}

	if len(s.Nodes) == 0 {
		return jerrors.Errorf("Require at least one node")
	}

	serviceKey := s.Path(r.options.Root)
	r.Lock()
	// delete our hash of the service
	delete(r.register, serviceKey)
	// delete our lease of the service
	delete(r.leases, serviceKey)
	r.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), r.options.Timeout)
	defer cancel()

	for _, node := range s.Nodes {
		_, err := r.client.EtcdClient().Delete(ctx, s.NodePath(r.options.Root, *node))
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Registry) GetService(attr gxregistry.ServiceAttr) ([]*gxregistry.Service, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.options.Timeout)
	defer cancel()

	svc := gxregistry.Service{ServiceAttr: attr}
	path := svc.Path(r.options.Root)
	rsp, err := r.client.EtcdClient().Get(
		ctx,
		path,
		etcdv3.WithPrefix(),
		etcdv3.WithSort(etcdv3.SortByKey, etcdv3.SortDescend),
	)
	if err != nil {
		return nil, err
	}

	if len(rsp.Kvs) == 0 {
		return nil, gxregistry.ErrorRegistryNotFound
	}

	serviceMap := map[gxregistry.ServiceAttr]*gxregistry.Service{}
	for _, n := range rsp.Kvs {
		if sn, err := gxregistry.DecodeService(n.Value); err == nil && sn != nil {
			s, ok := serviceMap[sn.ServiceAttr]
			if !ok {
				s = &gxregistry.Service{Metadata: sn.Metadata}
				s.ServiceAttr = sn.ServiceAttr
				serviceMap[s.ServiceAttr] = s
			}

			for _, node := range sn.Nodes {
				s.Nodes = append(s.Nodes, node)
			}
		}
	}

	var services []*gxregistry.Service
	for _, service := range serviceMap {
		services = append(services, service)
	}

	return services, nil
}

func (r *Registry) Watch(opts ...gxregistry.WatchOption) gxregistry.Watcher {
	return NewWatcher(r, opts...)
}

func (r *Registry) String() string {
	return "Etcdv3 Registry"
}

func (r *Registry) Close() error {
	r.Lock()
	for _, l := range r.leases {
		r.client.EtcdClient().Revoke(context.TODO(), l)
	}
	r.Unlock()

	err := r.client.Close()
	if err != nil {
		return jerrors.Annotate(err, "gxetcd.LeaseClient.Close()")
	}

	return nil
}

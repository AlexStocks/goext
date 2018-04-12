// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package etcdv3 provides an etcd version 3 gxregistry
// ref: https://github.com/micro/go-plugins/blob/master/registry/etcdv3/etcdv3.go
package etcdv3

import (
	"context"
	"errors"
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
)

type Registry struct {
	client  *gxetcd.LeaseClient
	options gxregistry.Options
	sync.Mutex
	register map[string]uint64
	leases   map[string]etcdv3.LeaseID
}

func (r *Registry) Close() error {
	err := r.client.Close()
	if err != nil {
		return jerrors.Annotate(err, "gxetcd.LeaseClient.Close()")
	}

	return nil
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

	var cAddrs []string

	for _, addr := range options.Addrs {
		if len(addr) == 0 {
			continue
		}
		cAddrs = append(cAddrs, addr)
	}

	// if we got addrs then we'll update
	if len(cAddrs) > 0 {
		config.Endpoints = cAddrs
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

func (r *Registry) Register(sv interface{}, opts ...gxregistry.RegisterOption) error {
	s, ok := sv.(*gxregistry.Service)
	if !ok {
		return jerrors.Errorf("@service:%+v type is not gxregistry.Service", sv)
	}
	if len(s.Nodes) == 0 {
		return errors.New("Require at least one node")
	}

	var leaseNotFound bool
	//refreshing lease if existing
	leaseID, ok := r.leases[s.Name]
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
	v, ok := r.register[s.Name]
	r.Unlock()

	// the service is unchanged, skip registering
	if ok && v == h && !leaseNotFound {
		return nil
	}

	service := &gxregistry.Service{
		Name:      s.Name,
		Version:   s.Version,
		Metadata:  s.Metadata,
		Endpoints: s.Endpoints,
	}

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
		if lgr != nil {
			_, err = r.client.EtcdClient().Put(
				ctx,
				gxregistry.NodePath(node.ID, r.options.Root, service.Name),
				gxregistry.EncodeService(service),
				etcdv3.WithLease(lgr.ID),
			)
		} else {
			_, err = r.client.EtcdClient().Put(
				ctx,
				gxregistry.NodePath(node.ID, r.options.Root, service.Name),
				gxregistry.EncodeService(service),
			)
		}
		if err != nil {
			return err
		}
	}

	r.Lock()
	// save our hash of the service
	r.register[s.Name] = h
	// save our leaseID of the service
	if lgr != nil {
		r.leases[s.Name] = lgr.ID
	}
	r.Unlock()

	return nil
}

func (r *Registry) Unregister(svc interface{}) error {
	s, ok := svc.(*gxregistry.Service)
	if !ok {
		return jerrors.Errorf("@service:%+v type is not gxregistry.Service", svc)
	}

	if len(s.Nodes) == 0 {
		return errors.New("Require at least one node")
	}

	r.Lock()
	// delete our hash of the service
	delete(r.register, s.Name)
	// delete our lease of the service
	delete(r.leases, s.Name)
	r.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), r.options.Timeout)
	defer cancel()

	for _, node := range s.Nodes {
		_, err := r.client.EtcdClient().Delete(ctx, gxregistry.NodePath(node.ID, r.options.Root, s.Name))
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Registry) GetService(name string) ([]*gxregistry.Service, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.options.Timeout)
	defer cancel()

	path := gxregistry.ServicePath(r.options.Root, name)
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

	serviceMap := map[string]*gxregistry.Service{}
	for _, n := range rsp.Kvs {
		if sn := gxregistry.DecodeService(n.Value); sn != nil {
			s, ok := serviceMap[sn.Version]
			if !ok {
				s = &gxregistry.Service{
					Name:      sn.Name,
					Version:   sn.Version,
					Metadata:  sn.Metadata,
					Endpoints: sn.Endpoints,
				}
				serviceMap[s.Version] = s
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

func (r *Registry) ListServices() ([]*gxregistry.Service, error) {
	var services []*gxregistry.Service
	nameSet := make(map[string]struct{})

	ctx, cancel := context.WithTimeout(context.Background(), r.options.Timeout)
	defer cancel()

	rsp, err := r.client.EtcdClient().Get(
		ctx,
		r.options.Root,
		etcdv3.WithPrefix(),
		etcdv3.WithSort(etcdv3.SortByKey, etcdv3.SortDescend),
	)
	if err != nil {
		return nil, err
	}

	if len(rsp.Kvs) == 0 {
		return []*gxregistry.Service{}, nil
	}

	for _, n := range rsp.Kvs {
		if sn := gxregistry.DecodeService(n.Value); sn != nil {
			nameSet[sn.Name] = struct{}{}
		}
	}
	for k := range nameSet {
		service := &gxregistry.Service{}
		service.Name = k
		services = append(services, service)
	}

	return services, nil
}

func (r *Registry) Watch(opts ...gxregistry.WatchOption) gxregistry.Watcher {
	return NewWatcher(r, r.options.Root, opts...)
}

func (r *Registry) String() string {
	return "Etcdv3 Registry"
}

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
)

import (
	"github.com/AlexStocks/goext/database/etcd"
	"github.com/AlexStocks/goext/database/registry"
)

type Registry struct {
	client  *gxetcd.LeaseClient
	options gxregistry.Options
	sync.Mutex
	register map[gxregistry.ServiceAttr]gxregistry.Service
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
		register: make(map[gxregistry.ServiceAttr]gxregistry.Service),
	}
}

func (r *Registry) Options() gxregistry.Options {
	return r.options
}

// 如果s.nodes为空，则返回当前registry中的service
// 若非空，则检查其中的每个node是否存在
func (r *Registry) exist(s gxregistry.Service) (gxregistry.Service, bool) {
	// get existing hash
	r.Lock()
	defer r.Unlock()
	v, ok := r.register[*(s.Attr)]
	if len(s.Nodes) == 0 {
		return v, ok
	}

	for i := range s.Nodes {
		flag := false
		for j := range v.Nodes {
			if s.Nodes[i].Equal(v.Nodes[j]) {
				flag = true
				continue
			}
		}
		if !flag {
			// log.Error("s.node:%s, v.nodes:%s", gxlog.PrettyString(s.Nodes[i]), gxlog.PrettyString(v.Nodes))
			return v, false
		}
	}

	return v, true
}

func (r *Registry) addService(s gxregistry.Service) {
	if len(s.Nodes) == 0 {
		return
	}

	// get existing hash
	r.Lock()
	defer r.Unlock()
	v, ok := r.register[*s.Attr]
	if !ok {
		r.register[*s.Attr] = s
		return
	}

	for i := range s.Nodes {
		flag := false
		for j := range v.Nodes {
			if s.Nodes[i].Equal(v.Nodes[j]) {
				flag = true
			}
		}
		if !flag {
			v.Nodes = append(v.Nodes, s.Nodes[i])
		}
	}
	r.register[*s.Attr] = v

	return
}

func (r *Registry) deleteService(s gxregistry.Service) {
	if len(s.Nodes) == 0 {
		return
	}

	// get existing hash
	r.Lock()
	defer r.Unlock()
	v, ok := r.register[*s.Attr]
	if !ok {
		return
	}

	for i := range s.Nodes {
		for j := range v.Nodes {
			if s.Nodes[i].Equal(v.Nodes[j]) {
				v.Nodes = append(v.Nodes[:j], v.Nodes[j+1:]...)
				break
			}
		}
	}
	r.register[*s.Attr] = v

	return
}

func (r *Registry) Register(s gxregistry.Service) error {
	if len(s.Nodes) == 0 {
		return jerrors.Errorf("Require at least one node")
	}

	if _, exist := r.exist(s); exist {
		return gxregistry.ErrorAlreadyRegister
	}

	service := gxregistry.Service{Metadata: s.Metadata}
	service.Attr = s.Attr

	ctx, cancel := context.WithTimeout(context.Background(), r.options.Timeout)
	defer cancel()

	// register every node
	for i, node := range s.Nodes {
		service.Nodes = []*gxregistry.Node{node}
		data, err := gxregistry.EncodeService(&service)
		if err != nil {
			service.Nodes = s.Nodes[:i]
			r.unregister(service)
			return jerrors.Annotatef(err, "gxregistry.EncodeService(service:%+v) = error:%s", service, err)
		}
		_, err = r.client.EtcdClient().Put(
			ctx,
			service.NodePath(r.options.Root, *node),
			data,
			etcdv3.WithLease(r.client.Lease()),
		)
		if err != nil {
			service.Nodes = s.Nodes[:i]
			r.unregister(service)
			return jerrors.Annotatef(err, "etcdv3.Client.Put(path, data)",
				service.NodePath(r.options.Root, *node), data)
		}
	}

	// save the service
	r.addService(s)
	// log.Debug("after add service, reg nodes:%s", gxlog.PrettyString(r.register))

	return nil
}

func (r *Registry) unregister(s gxregistry.Service) error {
	if len(s.Nodes) == 0 {
		return jerrors.Errorf("Require at least one node")
	}

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

func (r *Registry) Unregister(s gxregistry.Service) error {
	r.deleteService(s)
	return r.unregister(s)
}

func (r *Registry) GetService(attr gxregistry.ServiceAttr) (*gxregistry.Service, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.options.Timeout)
	defer cancel()

	svc := gxregistry.Service{Attr: &attr}
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

	service := &gxregistry.Service{Attr: &attr}
	for _, n := range rsp.Kvs {
		if sn, err := gxregistry.DecodeService(n.Value); err == nil && sn != nil {
			if sn.Attr.Equal(service.Attr) {
				for _, node := range sn.Nodes {
					service.Nodes = append(service.Nodes, node)
				}
			}
		}
	}

	return service, nil
}

func (r *Registry) Watch(opts ...gxregistry.WatchOption) gxregistry.Watcher {
	return NewWatcher(r, opts...)
}

func (r *Registry) String() string {
	return "Etcdv3 Registry"
}

func (r *Registry) Close() error {
	err := r.client.Close()
	if err != nil {
		return jerrors.Annotate(err, "gxetcd.LeaseClient.Close()")
	}

	return nil
}

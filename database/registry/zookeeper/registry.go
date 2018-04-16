// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxzookeeper provides a zookeeper registry
package gxzookeeper

import (
	"sync"
)

import (
	log "github.com/AlexStocks/log4go"
)

import (
	jerrors "github.com/juju/errors"
	hash "github.com/mitchellh/hashstructure"
)

import (
	"github.com/AlexStocks/goext/database/registry"
	"github.com/AlexStocks/goext/database/zookeeper"
)

//////////////////////////////////////////////
// Registry
//////////////////////////////////////////////

const (
	kRegistryZkClient = "zk registry"
)

type Registry struct {
	client     *gxzookeeper.Client
	options    gxregistry.Options
	sync.Mutex                   // lock for client + register
	register   map[string]uint64 // service name -> hash(gxregistry.Service)
}

func NewRegistry(opts ...gxregistry.Option) (*Registry, error) {
	var (
		err     error
		r       *Registry
		options gxregistry.Options
	)

	for _, o := range opts {
		o(&options)
	}
	if options.Timeout == 0 {
		options.Timeout = gxregistry.DefaultTimeout
	}
	if options.Root == "" {
		options.Root = gxregistry.DefaultServiceRoot
	}

	r = &Registry{options: options}
	err = r.validateZookeeperClient()
	if err != nil {
		return nil, err
	}
	r.register = make(map[string]uint64)

	return r, nil
}

func (r *Registry) Options() gxregistry.Options {
	return r.options
}

func (r *Registry) validateZookeeperClient() error {
	var err error
	r.Lock()
	if r.client == nil {
		r.client, err = gxzookeeper.NewClient(kRegistryZkClient, r.options.Addrs, r.options.Timeout)
		if err != nil {
			log.Warn("newZookeeperClient(name{%s}, zk addresss{%v}, timeout{%d}) = error{%v}",
				kRegistryZkClient, r.options.Addrs, r.options.Timeout, err)
		}
	}
	r.Unlock()

	return err
}

func (r *Registry) Register(sv interface{}, opts ...gxregistry.RegisterOption) error {
	s, ok := sv.(*gxregistry.Service)
	if !ok {
		return jerrors.Errorf("@service:%+v type is not gxregistry.Service", sv)
	}
	if len(s.Nodes) == 0 {
		return jerrors.Errorf("Require at least one node")
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
	if ok && v == h {
		return nil
	}

	service := &gxregistry.Service{Metadata: s.Metadata}
	service.ServiceAttr = s.ServiceAttr

	var options gxregistry.RegisterOptions
	for _, o := range opts {
		o(&options)
	}

	// register every node
	var zkPath string
	for _, node := range s.Nodes {
		r.Lock()
		defer r.Unlock()
		zkPath = service.NodePath(r.options.Root, *node)
		err = r.client.CreateZkPath(zkPath)
		if err != nil {
			log.Error("zkClient.CreateZkPath(root{%s})", zkPath, err)
			return err
		}
		service.Nodes = []*gxregistry.Node{node}
		sn, err := gxregistry.EncodeService(service)
		if err != nil {
			return jerrors.Annotatef(err, "gxregister.EncodeService(service:%+v)", service)
		}
		_, err = r.client.RegisterTempSeq(zkPath, []byte(sn))
		if err != nil {
			return jerrors.Annotatef(err, "gxregister.RegisterTempSeq(path:%s)", zkPath)
		}
	}

	r.Lock()
	// save our hash of the service
	r.register[s.Name] = h
	r.Unlock()

	return nil
}

func (r *Registry) Unregister(svc interface{}) error {
	s, ok := svc.(*gxregistry.Service)
	if !ok {
		return jerrors.Errorf("@service:%+v type is not gxregistry.Service", svc)
	}

	if len(s.Nodes) == 0 {
		return jerrors.Errorf("Require at least one node")
	}

	r.Lock()
	// delete our hash of the service
	delete(r.register, s.Name)
	r.Unlock()

	for _, node := range s.Nodes {
		if err := r.client.DeleteZkPath(s.NodePath(r.options.Root, *node)); err != nil {
			return jerrors.Annotatef(err, "zkClient.DeleteZkPath(%s)", s.NodePath(r.options.Root, *node))
		}
	}

	return nil
}

func (r *Registry) GetService(attr gxregistry.ServiceAttr) ([]*gxregistry.Service, error) {
	svc := gxregistry.Service{ServiceAttr: attr}
	path := svc.Path(r.options.Root)
	children, err := r.client.GetChildren(path)
	if err != nil {
		return nil, jerrors.Annotatef(err, "zkClient.GetChildren(path:%s)", path)
	}
	if len(children) == 0 {
		return nil, gxregistry.ErrorRegistryNotFound
	}

	serviceArray := []*gxregistry.Service{}
	var node gxregistry.Node
	for _, name := range children {
		node.ID = name
		zkPath := svc.NodePath(r.options.Root, node)
		grandchildren, err := r.client.GetChildren(zkPath)
		if err != nil {
			return nil, jerrors.Annotatef(err, "gxzookeeper.GetChildren(node:%+v)", node)
		}
		if len(grandchildren) > 0 {
			continue
		}

		childData, err := r.client.Get(zkPath)
		if err != nil {
			return nil, jerrors.Annotatef(err, "gxzookeeper.Get(name:%s)", zkPath)
		}

		sn, err := gxregistry.DecodeService(childData)
		if err != nil {
			return nil, jerrors.Annotate(err, "gxregistry.DecodeService()")
		}
		serviceArray = append(serviceArray, sn)
	}

	return serviceArray, nil
}

func (r *Registry) String() string {
	return "zookeeper registry"
}

func (r *Registry) Close() {
	r.client.Close()
}

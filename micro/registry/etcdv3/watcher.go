// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package etcdv3 provides an etcd version 3 gxregistry
// ref: https://github.com/micro/go-plugins/blob/master/gxregistry/etcdv3/etcdv3.go
package etcdv3

import (
	"context"
	"errors"
	"time"
)

import (
	"github.com/AlexStocks/goext/database/etcd"
	"github.com/AlexStocks/goext/micro/registry"
	"github.com/coreos/etcd/clientv3"
)

type Watcher struct {
	stop    chan struct{}
	w       clientv3.WatchChan
	client  *gxetcd.LeaseClient
	timeout time.Duration
}

func NewWatcher(r *Registry, timeout time.Duration, opts ...gxregistry.WatchOption) gxregistry.Watcher {
	var options gxregistry.WatchOptions
	for _, o := range opts {
		o(&options)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan struct{}, 1)

	go func() {
		<-stop
		cancel()
	}()

	watchPath := prefix
	if len(options.Service) > 0 {
		watchPath = servicePath(options.Service) + "/"
	}

	return &Watcher{
		stop:    stop,
		w:       r.client.EtcdClient().Watch(ctx, watchPath, clientv3.WithPrefix(), clientv3.WithPrevKV()),
		client:  r.client,
		timeout: timeout,
	}
}

func (w *Watcher) Next() (*gxregistry.Result, error) {
	for wresp := range w.w {
		if wresp.Err() != nil {
			return nil, wresp.Err()
		}
		for _, ev := range wresp.Events {
			service := decode(ev.Kv.Value)
			var action gxregistry.ServiceEventType

			switch ev.Type {
			case clientv3.EventTypePut:
				if ev.IsCreate() {
					action = gxregistry.ServiceAdd
				} else if ev.IsModify() {
					action = gxregistry.ServiceUpdate
				}
			case clientv3.EventTypeDelete:
				action = gxregistry.ServiceDel

				// get service from prevKv
				service = decode(ev.PrevKv.Value)
			}

			if service == nil {
				continue
			}
			return &gxregistry.Result{
				Action:  action,
				Service: service,
			}, nil
		}
	}
	return nil, errors.New("could not get next")
}

func (w *Watcher) Valid() bool {
	return w.client.TTL() > 0
}

func (w *Watcher) Stop() {
	select {
	case <-w.stop:
		return
	default:
		close(w.stop)
	}
}

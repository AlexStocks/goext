// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// Package gxetcd provides an etcd version 3 gxregistry
// ref: https://github.com/micro/go-plugins/blob/master/gxregistry/etcdv3/etcdv3.go
package gxetcd

import (
	"context"
)

import (
	"github.com/coreos/etcd/clientv3"
	jerrors "github.com/juju/errors"
)

import (
	"fmt"
	"github.com/AlexStocks/goext/database/etcd"
	"github.com/AlexStocks/goext/database/registry"
	log "github.com/AlexStocks/log4go"
)

// watcher的watch系列函数暴露给registry，而Next函数则暴露给selector
type Watcher struct {
	done   chan struct{}
	cancel context.CancelFunc
	w      clientv3.WatchChan
	opts   gxregistry.WatchOptions
	client *gxetcd.Client
}

func NewWatcher(client *gxetcd.Client, opts ...gxregistry.WatchOption) (gxregistry.Watcher, error) {
	var options gxregistry.WatchOptions
	for _, o := range opts {
		o(&options)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}, 1)

	if options.Root == "" {
		options.Root = gxregistry.DefaultServiceRoot
	}
	if options.Filter.Service == "" {
		return nil, jerrors.Errorf("options.Service is nil")
	}
	s := gxregistry.Service{
		Attr: &gxregistry.ServiceAttr{Service: options.Filter.Service},
	}
	watchPath := s.Path(options.Root)

	w := client.EtcdClient().Watch(ctx, watchPath, clientv3.WithPrefix(), clientv3.WithPrevKV())

	return &Watcher{
		done:   done,
		cancel: cancel,
		w:      w,
		client: client,
	}, nil
}

func (w *Watcher) Next() (*gxregistry.EventResult, error) {
	var action gxregistry.ServiceEventType
	for msg := range w.w {
		if w.IsClosed() {
			return nil, gxregistry.ErrWatcherClosed
		}

		if msg.Err() != nil {
			return nil, msg.Err()
		}

		for _, ev := range msg.Events {
			service, err := gxregistry.DecodeService(ev.Kv.Value)
			if err != nil {
				log.Warn("gxregistry.DecodeService() = error:%s", err)
				continue
			}

			if !w.opts.Filter.Filter(*service.Attr) {
				continue
			}

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
				service, err = gxregistry.DecodeService(ev.PrevKv.Value)
				if err != nil {
					log.Warn("gxregistry.DecodeService() = error:%s", err)
					continue
				}
			}

			if service == nil {
				continue
			}
			return &gxregistry.EventResult{
				Action:  action,
				Service: service,
			}, nil
		}
	}
	return nil, jerrors.Errorf("could not get next")
}

func (w *Watcher) Valid() bool {
	if w.IsClosed() {
		fmt.Println("fuck0")
		return false
	}

	return w.client.TTL() > 0
}

func (w *Watcher) Stop() {
	select {
	case <-w.done:
		return
	default:
		close(w.done)
		w.cancel()
	}
}

// check whether the session has been closed.
func (w *Watcher) IsClosed() bool {
	select {
	case <-w.done:
		return true

	default:
		return false
	}
}

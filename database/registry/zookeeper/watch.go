// Copyright 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of w source code is
// governed by Apache License 2.0.

// Package gxzookeeper provides a zookeeper watcher
package gxzookeeper

import (
	"fmt"
	"path"
	"sync"
)

import (
	log "github.com/AlexStocks/log4go"
	jerrors "github.com/juju/errors"
	"github.com/samuel/go-zookeeper/zk"
)

import (
	"github.com/AlexStocks/dubbogo/registry"
	"github.com/AlexStocks/goext/database/registry"
	"github.com/AlexStocks/goext/strings"
	"github.com/AlexStocks/goext/time"
	"time"
)

const (
	MAX_TIMES                   = 15 // 设置(wathcer)watchDir()等待时长
	Wactch_Event_Channel_Size   = 32 // 用于设置通知selector的event channel的size
	ZKCLIENT_EVENT_CHANNEL_SIZE = 4  // 设置用于zk client与watcher&consumer&provider之间沟通的channel的size
)

// watcher的watch系列函数暴露给zk registry，而Next函数则暴露给selector
type Watcher struct {
	opts   gxregistry.WatchOptions
	reg    *Registry
	events chan event // 通过这个channel把registry与selector连接了起来
	done   chan struct{}
	wg     sync.WaitGroup
}

type event struct {
	res *gxregistry.EventResult
	err error
}

func NewWatcher(r gxregistry.Registry, opts ...gxregistry.WatchOption) (gxregistry.Watcher, error) {
	reg, ok := r.(*Registry)
	if !ok {
		return nil, jerrors.Errorf("@r should be of type gxzookeeper.Registry", r)
	}

	var options gxregistry.WatchOptions
	for _, o := range opts {
		o(&options)
	}

	if options.Root == "" {
		options.Root = gxregistry.DefaultServiceRoot
	}

	w := &Watcher{
		opts:   options,
		reg:    reg,
		events: make(chan event, Wactch_Event_Channel_Size),
		done:   make(chan struct{}, 1),
	}

	go w.watchService()

	return w, nil
}

// 这个函数退出，意味着要么收到了stop信号，要么watch的node不存在了
func (w *Watcher) watchServiceNode(zkPath string) bool {
	w.wg.Add(1)
	defer w.wg.Done()

	var zkEvent zk.Event
	for {
		keyEventCh, err := w.reg.client.ExistW(zkPath)
		if err != nil {
			log.Error("existW{key:%s} = error{%#v}", zkPath, err)
			return false
		}

		select {
		case zkEvent = <-keyEventCh:
			log.Warn("get a zookeeper zkEvent{type:%s, server:%s, path:%s, state:%d-%s, err:%s}",
				zkEvent.Type, zkEvent.Server, zkEvent.Path, zkEvent.State, w.reg.client.StateToString(zkEvent.State), zkEvent.Err)
			switch zkEvent.Type {
			case zk.EventNodeDataChanged:
				log.Warn("zk.ExistW(key{%s}) = event{EventNodeDataChanged}", zkPath)
			case zk.EventNodeCreated:
				log.Warn("zk.ExistW(key{%s}) = event{EventNodeCreated}", zkPath)
			case zk.EventNotWatching:
				log.Warn("zk.ExistW(key{%s}) = event{EventNotWatching}", zkPath)
			case zk.EventNodeDeleted:
				log.Warn("zk.ExistW(key{%s}) = event{EventNodeDeleted}", zkPath)
				//The Node was deleted - stop watching
				return true
			}
		case <-w.done:
			// There is no way to stop existW so just quit
			return false
		}
	}

	return false
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}

	return false
}

func (w *Watcher) handleZkNodeEvent(zkPath string, children []string) {
	var (
		err         error
		newChildren []string
	)
	newChildren, err = w.reg.client.GetChildren(zkPath)
	if err != nil {
		log.Error("path{%s} child nodes changed, zk.Children(path{%s} = error{%v}", zkPath, zkPath, err)
		return
	}

	// a node was added -- watch the new node
	var (
		newNode string
		zkData  []byte
		conf    gxregistry.ServiceAttr
		service *gxregistry.Service
	)
	conf = w.opts.Filter
	for _, n := range newChildren {
		if contains(children, n) {
			continue
		}

		newNode = path.Join(zkPath, n)
		log.Info("add zkNode{%s}", newNode)
		zkData, err = w.reg.client.Get(newNode)
		if err != nil {
			log.Warn("can not get value of zk node %s", newNode)
			continue
		}
		service, err = gxregistry.DecodeService(zkData)
		if err != nil {
			log.Error("gxregistry.DecodeService(zkData:%s) = error{%v}", string(zkData), err)
			continue
		}

		if !conf.Filter(*service.Attr) {
			log.Warn("service{%#v} is not compatible with Config{%#v}", service, conf)
			continue
		}
		log.Info("add service{%#v}", service)
		w.events <- event{&gxregistry.EventResult{registry.ServiceURLAdd, service}, nil}
		// watch w service node
		go func(node string, service *gxregistry.Service) {
			log.Info("delete zkNode{%s}", node)
			// watch goroutine退出，原因可能是service node不存在或者是与registry连接断开了
			// 为了selector服务的稳定，仅在收到delete event的情况下向selector发送delete service event
			if w.watchServiceNode(node) {
				log.Info("delete service{%#v}", service)
				w.events <- event{&gxregistry.EventResult{registry.ServiceURLDel, service}, nil}
			}
			log.Warn("watchSelf(zk path{%s}) goroutine exit now", zkPath)
		}(newNode, service)
	}
}

// zkPath 是/dubbo/com.xxx.service
// 关注zk path下面node的添加或者删除
func (w *Watcher) watchDir(zkPath string) {
	var (
		err error
		//rootFlag     bool
		failTimes int
		event     chan struct{}
		zkEvent   zk.Event
		//children     []string
		newChildren  []string
		childEventCh <-chan zk.Event
	)

	if zkPath == w.opts.Root {
		//rootFlag = true
	}

	event = make(chan struct{}, ZKCLIENT_EVENT_CHANNEL_SIZE)

	w.wg.Add(1)
	defer func() {
		w.wg.Done()
		close(event)
	}()

	for {
		// get current newChildren for a zkPath
		newChildren, childEventCh, err = w.reg.client.GetChildrenW(zkPath)
		if err != nil {
			failTimes++
			if MAX_TIMES <= failTimes {
				failTimes = MAX_TIMES
			}
			log.Error("watchDir(path{%s}) = error{%v}", zkPath, err)
			// clear the event channel
		CLEAR:
			for {
				select {
				case <-event:
				default:
					break CLEAR
				}
			}
			//if rootFlag {
			//	for _,c := range newChildren {
			//		if !contains(children, c) {
			//			go  w.watchDir(path.Join(zkPath, c))
			//		}
			//	}
			//}

			w.reg.registerEvent(zkPath, &event)
			select {
			// 防止疯狂重试连接zookeeper
			case <-time.After(gxtime.TimeSecondDuration(float64(failTimes * gxregistry.REGISTRY_CONN_DELAY))):
				w.reg.unregisterEvent(zkPath, &event)
				continue
			case <-w.done:
				w.reg.unregisterEvent(zkPath, &event)
				log.Warn("client.done(), watch(path{%s}, ServiceConfig{%#v}) goroutine exit now...",
					zkPath, w.opts.Filter)
				return
			case <-event:
				log.Info("get zk.EventNodeDataChange notify event")
				w.reg.unregisterEvent(zkPath, &event)
				w.handleZkNodeEvent(zkPath, nil)
				continue
			}
		}
		failTimes = 0

		select {
		case zkEvent = <-childEventCh:
			log.Warn("get a zookeeper zkEvent {type:%s, server:%s, path:%s, state:%d-%s, err:%#v}",
				zkEvent.Type, zkEvent.Server, zkEvent.Path, zkEvent.State,
				w.reg.client.StateToString(zkEvent.State), zkEvent.Err)
			if zkEvent.Type != zk.EventNodeChildrenChanged {
				continue
			}
			w.handleZkNodeEvent(zkEvent.Path, newChildren)
		case <-w.reg.done:
			// There is no way to stop GetW/ChildrenW so just quit
			log.Warn("client.done(), watch(path{%s}, ServiceConfig{%#v}) goroutine exit now...",
				zkPath, w.opts.Filter)
			return
		}

		//children = newChildren
	}
}

// watchService -> watchDir -> watchServiceNode
func (w *Watcher) watchService() {
	var (
		err         error
		servicePath string
		children    []string
		zkPath      string
		attr        gxregistry.ServiceAttr
	)

	zkPath = w.opts.Root
	if len(zkPath) == 0 {
		return
	}

	// 先把现有的服务节点通过watch发送给selector
	children, err = w.reg.client.GetChildren(zkPath)
	if err != nil {
		children = nil
		log.Error("fail to get children of zk path{%s}", zkPath)
		// 不要发送不必要的error给selector，以防止selector/cache/cache.go:(cacheSelector)watch
		// 调用(Watcher)Next获取error后，不断退出
		// w.events <- event{nil, err}
	}
	fmt.Println("zk path ", zkPath, ", children: ", children)

	for _, c := range children {
		if err = attr.UnmarshalPath(gxstrings.Slice(c)); err != nil {
			log.Warn("ServiceAttr.UnmarshalPath() = error:%s", jerrors.ErrorStack(err))
			continue
		}
		if !w.opts.Filter.Filter(attr) {
			log.Warn("@w.opts.Filter:%#v, path attr:%#v", w.opts.Filter, attr)
			continue
		}

		// watch w service node
		log.Info("watch service key{%s}", servicePath)
		servicePath = path.Join(zkPath, c)
		go w.watchDir(servicePath)
	}
	//go w.watchDir(zkPath)
}

func (w *Watcher) Notify() (*gxregistry.EventResult, error) {
	select {
	case <-w.done:
		return nil, jerrors.New("watcher stopped")

	case r := <-w.events:
		return r.res, r.err
	}
}

func (w *Watcher) Valid() bool {
	if w.IsClosed() {
		return false
	}

	select {
	case <-w.reg.done:
		return false

	default:
		zkState := w.reg.client.ZkConn().State()
		//fmt.Println("state:", w.reg.client.StateToString(zkState))
		if zkState == zk.StateConnected || zkState == zk.StateHasSession {
			return true
		}

		return false
	}
}

func (w *Watcher) Close() {
	if !w.IsClosed() {
		close(w.done)
	}

	w.wg.Wait()
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

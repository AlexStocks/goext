/******************************************************
# DESC    : zookeeper path watcher
# AUTHOR  : Alex Stocks
# VERSION : 1.0
# LICENCE : Apache Licence 2.0
# EMAIL   : alexstocks@foxmail.com
# MOD     : 2016-06-26 19:30
# FILE    : watch.go
******************************************************/

package zookeeper

import (
	"errors"
	"path"
	"sync"
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
	"github.com/samuel/go-zookeeper/zk"
)

import (
	"github.com/AlexStocks/dubbogo/common"
	"github.com/AlexStocks/dubbogo/registry"
)

const (
	MAX_TIMES                   = 15 // 设置(wathcer)watchDir()等待时长
	Wactch_Event_Channel_Size   = 32 // 用于设置通知selector的event channel的size
	ZKCLIENT_EVENT_CHANNEL_SIZE = 4  // 设置用于zk client与watcher&consumer&provider之间沟通的channel的size
)

// watcher的watch系列函数暴露给zk registry，而Next函数则暴露给selector
type zookeeperWatcher struct {
	once   sync.Once
	client *zookeeperClient
	events chan event // 通过这个channel把registry与selector连接了起来
	wait   sync.WaitGroup
}

type event struct {
	res *registry.Result
	err error
}

func newZookeeperWatcher(client *zookeeperClient) (registry.Watcher, error) {
	this := &zookeeperWatcher{
		client: client,
		events: make(chan event, Wactch_Event_Channel_Size),
	}

	return this, nil
}

// 这个函数退出，意味着要么收到了stop信号，要么watch的node不存在了
// 除了下面的watchDir会调用这个函数外，func (this *zookeeperRegistry) registerZookeeperNode(root string, data []byte)也
// 调用了这个函数
func (this *zookeeperWatcher) watchServiceNode(zkPath string) bool {
	this.wait.Add(1)
	defer this.wait.Done()
	var zkEvent zk.Event
	for {
		keyEventCh, err := this.client.existW(zkPath)
		if err != nil {
			log.Error("existW{key:%s} = error{%v}", zkPath, err)
			return false
		}

		select {
		case zkEvent = <-keyEventCh:
			log.Warn("get a zookeeper zkEvent{type:%s, server:%s, path:%s, state:%d-%s, err:%s}",
				zkEvent.Type.String(), zkEvent.Server, zkEvent.Path, zkEvent.State, stateToString(zkEvent.State), zkEvent.Err)
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
		case <-this.client.done():
			// There is no way to stop existW so just quit
			return false
		}
	}

	return false
}

func (this *zookeeperWatcher) handleZkNodeEvent(zkPath string, children []string, conf registry.ServiceConfig) {
	var (
		err         error
		newChildren []string
	)
	newChildren, err = this.client.getChildren(zkPath)
	if err != nil {
		log.Error("path{%s} child nodes changed, zk.Children(path{%s} = error{%v}", zkPath, zkPath, err)
		return
	}

	// a node was added -- watch the new node
	var (
		newNode    string
		serviceURL *registry.ServiceURL
	)
	for _, n := range newChildren {
		if common.Contains(children, n) {
			continue
		}

		newNode = path.Join(zkPath, n)
		log.Info("add zkNode{%s}", newNode)
		serviceURL, err = registry.NewServiceURL(n)
		if err != nil {
			log.Error("NewServiceURL(%s) = error{%v}", n, err)
			continue
		}
		if !conf.ServiceEqual(serviceURL) {
			log.Warn("serviceURL{%#v} is not compatible with ServiceConfig{%#v}", serviceURL, conf)
			continue
		}
		log.Info("add serviceURL{%#v}", serviceURL)
		this.events <- event{&registry.Result{registry.ServiceURLAdd, serviceURL}, nil}
		// watch this service node
		go func(node string, serviceURL *registry.ServiceURL) {
			log.Info("delete zkNode{%s}", node)
			// watch goroutine退出，原因可能是service node不存在或者是与registry连接断开了
			// 为了selector服务的稳定，仅在收到delete event的情况下向selector发送delete service event
			if this.watchServiceNode(node) {
				log.Info("delete serviceURL{%#v}", serviceURL)
				this.events <- event{&registry.Result{registry.ServiceURLDel, serviceURL}, nil}
			}
			log.Warn("watchSelf(zk path{%s}) goroutine exit now", zkPath)
		}(newNode, serviceURL)
	}

	// old node was deleted
	// 因为有上面的goroutine关注node的删除，一旦node不存在，上面这个routine会第一时间感知到,所以这个循环检测到的node会
	// 导致selector两次收到node的删除通知结果
	var oldNode string
	for _, n := range children {
		if common.Contains(newChildren, n) {
			continue
		}

		oldNode = path.Join(zkPath, n)
		log.Warn("delete zkPath{%s}", oldNode)
		serviceURL, err = registry.NewServiceURL(n)
		if !conf.ServiceEqual(serviceURL) {
			log.Warn("serviceURL{%#v} has been deleted is not compatible with ServiceConfig{%#v}", serviceURL, conf)
			continue
		}
		log.Warn("delete serviceURL{%#v}", serviceURL)
		if err != nil {
			log.Error("NewServiceURL(i{%s}) = error{%v}", n, err)
			continue
		}
		this.events <- event{&registry.Result{registry.ServiceURLDel, serviceURL}, nil}
	}
}

// zkPath 是/dubbo/com.xxx.service/[providers or consumers or configurators]
// 关注zk path下面node的添加或者删除
func (this *zookeeperWatcher) watchDir(zkPath string, conf registry.ServiceConfig) {
	this.wait.Add(1)
	defer this.wait.Done()

	var (
		failTimes int
		event     chan struct{}
		zkEvent   zk.Event
	)
	event = make(chan struct{}, ZKCLIENT_EVENT_CHANNEL_SIZE)
	defer close(event)
	for {
		// get current children for a zkPath
		children, childEventCh, err := this.client.getChildrenW(zkPath)
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
			this.client.registerEvent(zkPath, &event)
			select {
			// 防止疯狂重试连接zookeeper
			case <-time.After(common.TimeSecondDuration(failTimes * registry.REGISTRY_CONN_DELAY)):
				this.client.unregisterEvent(zkPath, &event)
				continue
			case <-this.client.done():
				this.client.unregisterEvent(zkPath, &event)
				log.Warn("client.done(), watch(path{%s}, ServiceConfig{%#v}) goroutine exit now...", zkPath, conf)
				return
			case <-event:
				log.Info("get zk.EventNodeDataChange notify event")
				this.client.unregisterEvent(zkPath, &event)
				this.handleZkNodeEvent(zkPath, nil, conf)
				continue
			}
		}
		failTimes = 0

		select {
		case zkEvent = <-childEventCh:
			log.Warn("get a zookeeper zkEvent{type:%s, server:%s, path:%s, state:%d-%s, err:%s}",
				zkEvent.Type.String(), zkEvent.Server, zkEvent.Path, zkEvent.State, stateToString(zkEvent.State), zkEvent.Err)
			if zkEvent.Type != zk.EventNodeChildrenChanged {
				continue
			}
			this.handleZkNodeEvent(zkEvent.Path, children, conf)
		case <-this.client.done():
			// There is no way to stop GetW/ChildrenW so just quit
			log.Warn("client.done(), watch(path{%s}, ServiceConfig{%#v}) goroutine exit now...", zkPath, conf)
			return
		}
	}
}

// watich.go:watchService暴露给client.go:Watch,其他函数都会被这个函数调用到
// client.go:Watch -> watchService -> watchDir -> watchServiceNode
//                            |
//                            --------> watchServiceNode
func (this *zookeeperWatcher) watchService(zkPath string, conf registry.ServiceConfig) {
	var (
		err        error
		dubboPath  string
		children   []string
		serviceURL *registry.ServiceURL
	)

	// 先把现有的服务节点通过watch发送给selector
	children, err = this.client.getChildren(zkPath)
	if err != nil {
		children = nil
		log.Error("fail to get children of zk path{%s}", zkPath)
		// 不要发送不必要的error给selector，以防止selector/cache/cache.go:(cacheSelector)watch
		// 调用(zookeeperWatcher)Next获取error后，不断退出
		// this.events <- event{nil, err}
	}

	for _, c := range children {
		serviceURL, err = registry.NewServiceURL(c)
		if err != nil {
			log.Error("NewServiceURL(r{%s}) = error{%v}", c, err)
			continue
		}
		// 此处暂不把 service.ServiceConfig.service 和 serviceURL.Query["interface"] 进行比较，一般情况下service=interface+version
		// 因为service.ServiceConfig.service指代的是"/dubbo/com.xxx.xxx"中的"com.xxx.xxx"
		// if serviceURL.Protocol != conf.Protocol || serviceURL.Group != conf.Group || serviceURL.Version != conf.Version {
		if !conf.ServiceEqual(serviceURL) {
			log.Warn("serviceURL{%#v} is not compatible with ServiceConfig{%#v}", serviceURL, conf)
			continue
		}
		log.Debug("add serviceUrl{%#v}", serviceURL)
		this.events <- event{&registry.Result{registry.ServiceURLAdd, serviceURL}, nil}

		// watch this service node
		dubboPath = path.Join(zkPath, c)
		log.Info("watch dubbo service key{%s}", dubboPath)
		go func(zkPath string, serviceURL *registry.ServiceURL) {
			if this.watchServiceNode(dubboPath) {
				log.Debug("delete serviceUrl{%#v}", serviceURL)
				this.events <- event{&registry.Result{registry.ServiceURLDel, serviceURL}, nil}
			}
			log.Warn("watchSelf(zk path{%s}) goroutine exit now", zkPath)
		}(dubboPath, serviceURL)
	}

	log.Info("watch dubbo path{%s}", zkPath)
	go func(zkPath string, conf registry.ServiceConfig) {
		this.watchDir(zkPath, conf)
		log.Warn("watchDir(zkPath{%s}) goroutine exit now", zkPath)
	}(zkPath, conf)
}

func (this *zookeeperWatcher) Next() (*registry.Result, error) {
	select {
	case <-this.client.done():
		return nil, errors.New("watcher stopped")
	case r := <-this.events:
		return r.res, r.err
	}
}

func (this *zookeeperWatcher) Valid() bool {
	return this.client.zkConnValid()
}

func (this *zookeeperWatcher) Stop() {
	this.once.Do(func() {
		this.client.Close()
	})
}

/******************************************************
# DESC    : service registry
# AUTHOR  : Alex Stocks
# VERSION : 1.0
# LICENCE : Apache Licence 2.0
# EMAIL   : alexstocks@foxmail.com
# MOD     : 2016-06-08 19:23
# FILE    : registry.go
******************************************************/

package zookeeper

import (
	"fmt"
	"os"
	"sync"
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
)

import (
	"github.com/AlexStocks/dubbogo/common"
	"github.com/AlexStocks/dubbogo/registry"
)

//////////////////////////////////////////////
// DubboType
//////////////////////////////////////////////

type DubboType int

const (
	CONSUMER = iota
	CONFIGURATOR
	ROUTER
	PROVIDER
)

var (
	DubboNodes              = [...]string{"consumers", "configurators", "routers", "providers"}
	DubboRole               = [...]string{"consumer", "", "", "provider"} //
	RegistryZkClient string = "zk registry"
	processID               = ""
	localIp                 = ""
)

func init() {
	processID = fmt.Sprintf("%d", os.Getpid())
	localIp, _ = common.GetLocalIP(localIp)
}

func (t DubboType) String() string {
	return DubboNodes[t]
}

func (t DubboType) Role() string {
	return DubboRole[t]
}

//////////////////////////////////////////////
// zookeeperRegistry
//////////////////////////////////////////////

const (
	DEFAULT_REGISTRY_TIMEOUT = 1
)

type serviceZKPath struct {
	path string
	node string
}

// 从目前消费者的功能来看，它实现:
// 1 消费者在每个服务下的/dubbo/service/consumers下注册
// 2 消费者watch /dubbo/service/providers变动
// 3 zk连接创建的时候，监控连接的可用性
type zookeeperRegistry struct {
	common.ApplicationConfig
	registry.RegistryConfig                // ZooKeeperServers []string
	birth                   int64          // time of file birth, seconds since Epoch; 0 if unknown
	wg                      sync.WaitGroup // wg+done for zk restart
	done                    chan struct{}
	// watcher *zookeeperWatcher
	sync.Mutex // lock for client + services
	client     *zookeeperClient
	services   map[string]registry.ServiceConfigIf // service name -> service config
	// zkPath -> zkData, 存储了当前使用者在各个服务下面注册的node,
	// 如果注册了temp node，则zkData为空，如果注册了temp seq node,则zkData非空
	// registers map[string]string
}

func newZookeeperRegistry(opts registry.Options) (*zookeeperRegistry, error) {
	var (
		// ok       bool
		err  error
		this *zookeeperRegistry
	)

	this = &zookeeperRegistry{
		RegistryConfig:    opts.RegistryConfig,
		ApplicationConfig: opts.ApplicationConfig,
		birth:             time.Now().Unix(),
		done:              make(chan struct{}),
	}
	if this.Name == "" {
		this.Name = common.NAME
	}
	if this.Version == "" {
		this.Version = common.VERSION
	}
	if this.RegistryConfig.Timeout == 0 {
		this.RegistryConfig.Timeout = DEFAULT_REGISTRY_TIMEOUT
	}
	err = this.validateZookeeperClient()
	if err != nil {
		return nil, err
	}

	// this.registers = make(map[string]string)
	this.services = make(map[string]registry.ServiceConfigIf)

	return this, nil
}

func (this *zookeeperRegistry) validateZookeeperClient() error {
	var (
		err error
	)

	err = nil
	this.Lock()
	if this.client == nil {
		this.client, err = newZookeeperClient(RegistryZkClient, this.Address, this.RegistryConfig.Timeout)
		if err != nil {
			log.Warn("newZookeeperClient(name{%s}, zk addresss{%v}, timeout{%d}) = error{%v}",
				RegistryZkClient, this.Address, this.Timeout, err)
		}
	}
	this.Unlock()

	return err
}

func (this *zookeeperRegistry) GetService(string) ([]*registry.ServiceURL, error) {
	return nil, nil
}

func (this *zookeeperRegistry) ListServices() ([]*registry.ServiceURL, error) {
	return nil, nil
}

func (this *zookeeperRegistry) Watch() (registry.Watcher, error) {
	return nil, nil
}

func (this *zookeeperRegistry) Close() {
	this.client.Close()
}

func (this *zookeeperRegistry) registerZookeeperNode(root string, data []byte) error {
	var (
		err    error
		zkPath string
	)

	// 假设root是/dubbo/com.ofpay.demo.api.UserProvider/consumers/jsonrpc，则创建完成的时候zkPath
	// 是/dubbo/com.ofpay.demo.api.UserProvider/consumers/jsonrpc/0000000000之类的临时节点.
	// 这个节点在连接有效的时候回一直存在，直到退出的时候才会被删除。
	// 所以如果连接有效的话，想要删除/dubbo/com.ofpay.demo.api.UserProvider/consumers/jsonrpc的话，必须先把这个临时节点删除掉
	this.Lock()
	defer this.Unlock()
	err = this.client.Create(root)
	if err != nil {
		log.Error("zk.Create(root{%s}) = err{%v}", root, err)
		return err
	}
	zkPath, err = this.client.RegisterTempSeq(root, data)
	// 创建完临时节点，zkPath = /dubbo/com.ofpay.demo.api.UserProvider/consumers/jsonrpc/0000000000
	if err != nil {
		log.Error("createTempSeqNode(root{%s}) = error{%v}", root, err)
		return err
	}
	// this.registers[root] = string(data) // root = /dubbo/com.ofpay.demo.api.UserProvider/consumers/jsonrpc
	log.Debug("create a zookeeper node:%s", zkPath)

	return nil
}

func (this *zookeeperRegistry) registerTempZookeeperNode(root string, node string) error {
	var (
		err    error
		zkPath string
	)

	this.Lock()
	defer this.Unlock()
	err = this.client.Create(root)
	if err != nil {
		log.Error("zk.Create(root{%s}) = err{%v}", root, err)
		return err
	}
	zkPath, err = this.client.RegisterTemp(root, node)
	if err != nil {
		log.Error("RegisterTempNode(root{%s}, node{%s}) = error{%v}", root, node, err)
		return err
	}
	// this.registers[zkPath] = ""
	log.Debug("create a zookeeper node:%s", zkPath)

	return nil
}

func (this *zookeeperRegistry) Register(conf registry.ServiceConfig) error {
	return nil
}

func (this *zookeeperRegistry) String() string {
	return "zookeeper registry"
}

/******************************************************
# DESC    : zookeeper provider registry
# AUTHOR  : Alex Stocks
# VERSION : 1.0
# LICENCE : Apache Licence 2.0
# EMAIL   : alexstocks@foxmail.com
# MOD     : 2016-06-17 11:06
# FILE    : server.go
******************************************************/

package zookeeper

import (
	"fmt"
	"net/url"
	"strconv"
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
)

import (
	"github.com/AlexStocks/dubbogo/common"
	"github.com/AlexStocks/dubbogo/registry"
	"github.com/AlexStocks/dubbogo/version"
)

const (
	ProviderRegistryZkClient string = "consumer zk registry"
)

type providerZookeeperRegistry struct {
	*zookeeperRegistry
	zkPath map[string]int // key = protocol://ip:port/interface
}

func NewProviderZookeeperRegistry(opts ...registry.Option) registry.Registry {
	var (
		err     error
		options registry.Options
		reg     *zookeeperRegistry
		this    *providerZookeeperRegistry
	)

	options = registry.Options{}
	for _, opt := range opts {
		opt(&options)
	}

	reg, err = newZookeeperRegistry(options)
	if err != nil {
		return nil
	}
	reg.client.name = ProviderRegistryZkClient
	this = &providerZookeeperRegistry{zookeeperRegistry: reg, zkPath: make(map[string]int)}
	this.wg.Add(1)
	go this.handleZkRestart()

	return this
}

func (this *providerZookeeperRegistry) validateZookeeperClient() error {
	var (
		err error
	)

	err = nil
	this.Lock()
	if this.client == nil {
		this.client, err = newZookeeperClient(ProviderRegistryZkClient, this.Address, this.RegistryConfig.Timeout)
		if err != nil {
			log.Warn("newZookeeperClient(name{%s}, zk addresss{%v}, timeout{%d}) = error{%v}",
				ProviderRegistryZkClient, this.Address, this.Timeout, err)
		}
	}
	this.Unlock()

	return err
}

func (this *providerZookeeperRegistry) Register(c interface{}) error {
	var (
		ok   bool
		err  error
		conf registry.ProviderServiceConfig
	)

	if conf, ok = c.(registry.ProviderServiceConfig); !ok {
		return fmt.Errorf("@c{%v} type is not registry.ServiceConfig", c)
	}

	// 检验服务是否已经注册过
	ok = false
	this.Lock()
	// 注意此处与consumerZookeeperRegistry的差异，consumer用的是conf.Service，因为consumer要提供watch功能给selector使用
	// provider允许注册同一个service的多个group or version
	_, ok = this.services[conf.String()]
	this.Unlock()
	if ok {
		return fmt.Errorf("Service{%s} has been registered", conf.String())
	}

	err = this.register(&conf)
	if err != nil {
		return err
	}

	this.Lock()
	this.services[conf.String()] = &conf
	log.Debug("(providerZookeeperRegistry)Register(conf{%#v})", conf)
	this.Unlock()

	return nil
}

func (this *providerZookeeperRegistry) register(conf *registry.ProviderServiceConfig) error {
	var (
		err        error
		revision   string
		params     url.Values
		urlPath    string
		rawURL     string
		encodedURL string
		dubboPath  string
	)

	if conf.ServiceConfig.Service == "" || conf.Methods == "" {
		return fmt.Errorf("conf{Service:%s, Methods:%s}", conf.ServiceConfig.Service, conf.Methods)
	}

	err = this.validateZookeeperClient()
	if err != nil {
		return err
	}
	// 先创建服务下面的provider node
	dubboPath = fmt.Sprintf("/dubbo/%s/%s", conf.Service, DubboNodes[PROVIDER])
	this.Lock()
	err = this.client.Create(dubboPath)
	this.Unlock()
	if err != nil {
		log.Error("zkClient.create(path{%s}) = error{%v}", dubboPath, err)
		return err
	}

	params = url.Values{}
	params.Add("interface", conf.ServiceConfig.Service)
	params.Add("application", this.ApplicationConfig.Name)
	revision = this.ApplicationConfig.Version
	if revision == "" {
		revision = "0.1.0"
	}
	params.Add("revision", revision) // revision是pox.xml中application的version属性的值
	if conf.ServiceConfig.Group != "" {
		params.Add("group", conf.ServiceConfig.Group)
	}
	// dubbo java consumer来启动找provider url时，因为category不匹配，会找不到provider，导致consumer启动不了,所以使用consumers&providers
	// DubboRole               = [...]string{"consumer", "", "", "provider"}
	// params.Add("category", (DubboType(PROVIDER)).Role())
	params.Add("category", (DubboType(PROVIDER)).String())
	params.Add("dubbo", "dubbo-provider-golang-"+version.Version)
	params.Add("org", this.ApplicationConfig.Organization)
	params.Add("module", this.ApplicationConfig.Module)
	params.Add("owner", this.ApplicationConfig.Owner)
	params.Add("side", (DubboType(PROVIDER)).Role())
	params.Add("pid", processID)
	params.Add("ip", localIp)
	params.Add("timeout", fmt.Sprintf("%v", this.Timeout))
	// params.Add("timestamp", time.Now().Format("20060102150405"))
	params.Add("timestamp", fmt.Sprintf("%d", this.birth))
	if conf.ServiceConfig.Version != "" {
		params.Add("version", conf.ServiceConfig.Version)
	}
	if conf.Methods != "" {
		params.Add("methods", conf.Methods)
	}
	log.Debug("provider zk url params:%#v", params)
	if conf.Path == "" {
		conf.Path = localIp
	}

	urlPath = conf.Service
	if this.zkPath[urlPath] != 0 {
		urlPath += strconv.Itoa(this.zkPath[urlPath])
	}
	this.zkPath[urlPath]++
	rawURL = fmt.Sprintf("%s://%s/%s?%s", conf.Protocol, conf.Path, urlPath, params.Encode())

	log.Debug("provider url:%s", rawURL)
	encodedURL = url.QueryEscape(rawURL)
	log.Debug("url.QueryEscape(consumer url:%s) = %s", rawURL, encodedURL)

	// 把自己注册service providers
	dubboPath = fmt.Sprintf("/dubbo/%s/%s", conf.Service, (DubboType(PROVIDER)).String())
	err = this.registerTempZookeeperNode(dubboPath, encodedURL)
	if err != nil {
		return err
	}

	return nil
}

func (this *providerZookeeperRegistry) handleZkRestart() {
	var (
		err       error
		flag      bool
		failTimes int
		confIf    registry.ServiceConfigIf
		services  []registry.ServiceConfigIf
	)

	defer this.wg.Done()
LOOP:
	for {
		select {
		case <-this.done:
			log.Warn("(providerZookeeperRegistry)reconnectZkRegistry goroutine exit now...")
			break LOOP
			// re-register all services
		case <-this.client.done():
			this.Lock()
			this.client.Close()
			this.client = nil
			this.Unlock()

			// 接zk，直至成功
			failTimes = 0
			for {
				select {
				case <-this.done:
					log.Warn("(providerZookeeperRegistry)reconnectZkRegistry goroutine exit now...")
					break LOOP
				case <-time.After(common.TimeSecondDuration(failTimes * registry.REGISTRY_CONN_DELAY)): // 防止疯狂重连zk
				}
				err = this.validateZookeeperClient()
				log.Info("providerZookeeperRegistry.validateZookeeperClient(zkAddr{%s}) = error{%#v}", this.client.zkAddrs, err)
				if err == nil {
					// copy this.services
					this.Lock()
					for _, confIf = range this.services {
						services = append(services, confIf)
					}
					this.Unlock()

					flag = true
					for _, confIf = range services {
						err = this.register(confIf.(*registry.ProviderServiceConfig))
						if err != nil {
							log.Error("in (providerZookeeperRegistry)reRegister, (providerZookeeperRegistry)register(conf{%#v}) = error{%#v}",
								confIf.(*registry.ProviderServiceConfig), err)
							flag = false
							break
						}
					}
					if flag {
						break
					}
				}
				failTimes++
				if MAX_TIMES <= failTimes {
					failTimes = MAX_TIMES
				}
			}
		}
	}
}

func (this *providerZookeeperRegistry) String() string {
	return "dubbogo rpc provider zookeeper registry"
}

func (this *providerZookeeperRegistry) closeRegisters() {
	var (
		// err error
		key string
	)

	this.Lock()
	log.Info("begin to close provider zk client")
	// 先关闭旧client，以关闭tmp node
	this.client.Close()
	this.client = nil
	// for key = range this.registers {
	// 	log.Debug("delete register provider zk path:%s, err = %v\n", key, err)
	// 	// do not delete related zk path, 'cause it's a temp zk node path
	// 	// delete(this.registers, key)
	// }
	// this.registers = nil
	for key = range this.services {
		log.Debug("delete register provider zk path:%s", key)
		// 	delete(this.services, key)
	}
	this.services = nil
	this.Unlock()
}

func (this *providerZookeeperRegistry) Close() {
	close(this.done)
	this.wg.Wait()
	this.closeRegisters()
}

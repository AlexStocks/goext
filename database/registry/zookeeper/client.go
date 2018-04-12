/******************************************************
# DESC    : zookeeper consumer registry
# AUTHOR  : Alex Stocks
# VERSION : 1.0
# LICENCE : Apache Licence 2.0
# EMAIL   : alexstocks@foxmail.com
# MOD     : 2016-06-17 11:06
# FILE    : client.go
******************************************************/

package zookeeper

import (
	"fmt"
	"net/url"
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
	ConsumerRegistryZkClient string = "consumer zk registry"
	WatcherZkClient          string = "watcher zk registry"
)

type consumerZookeeperRegistry struct {
	*zookeeperRegistry
}

func NewConsumerZookeeperRegistry(opts ...registry.Option) registry.Registry {
	var (
		err     error
		options registry.Options
		reg     *zookeeperRegistry
		this    *consumerZookeeperRegistry
	)

	options = registry.Options{}
	for _, opt := range opts {
		opt(&options)
	}

	reg, err = newZookeeperRegistry(options)
	if err != nil {
		return nil
	}
	reg.client.name = ConsumerRegistryZkClient
	this = &consumerZookeeperRegistry{zookeeperRegistry: reg}
	this.wg.Add(1)
	go this.handleZkRestart()

	return this
}

func (this *consumerZookeeperRegistry) validateZookeeperClient() error {
	var (
		err error
	)

	err = nil
	this.Lock()
	if this.client == nil {
		this.client, err = newZookeeperClient(ConsumerRegistryZkClient, this.Address, this.RegistryConfig.Timeout)
		if err != nil {
			log.Warn("newZookeeperClient(name{%s}, zk addresss{%v}, timeout{%d}) = error{%v}",
				ConsumerRegistryZkClient, this.Address, this.Timeout, err)
		}
	}
	this.Unlock()

	return err
}

func (this *consumerZookeeperRegistry) Register(c interface{}) error {
	var (
		ok   bool
		err  error
		conf registry.ServiceConfig
	)

	if conf, ok = c.(registry.ServiceConfig); !ok {
		return fmt.Errorf("@c{%v} type is not registry.ServiceConfig", c)
	}

	// 检验服务是否已经注册过
	ok = false
	this.Lock()
	// 注意此处与providerZookeeperRegistry的差异，provider用的是conf.String()，因为provider无需提供watch功能给selector使用
	// consumer只允许把service的其中一个group&version提供给用户使用
	// _, ok = this.services[conf.String()]
	_, ok = this.services[conf.Service]
	this.Unlock()
	if ok {
		return fmt.Errorf("Service{%s} has been registered", conf.Service)
	}

	err = this.register(&conf)
	if err != nil {
		return err
	}

	this.Lock()
	// this.services[conf.String()] = &conf
	this.services[conf.Service] = &conf
	log.Debug("(consumerZookeeperRegistry)Register(conf{%#v})", conf)
	this.Unlock()

	return nil
}

func (this *consumerZookeeperRegistry) register(conf *registry.ServiceConfig) error {
	var (
		err        error
		params     url.Values
		revision   string
		rawURL     string
		encodedURL string
		dubboPath  string
	)

	err = this.validateZookeeperClient()
	if err != nil {
		return err
	}
	// 创建服务下面的consumer node
	dubboPath = fmt.Sprintf("/dubbo/%s/%s", conf.Service, DubboNodes[CONSUMER])
	this.Lock()
	err = this.client.Create(dubboPath)
	this.Unlock()
	if err != nil {
		log.Error("zkClient.create(path{%s}) = error{%v}", dubboPath, err)
		return err
	}
	// 创建服务下面的provider node，以方便watch直接观察provider下面的新注册的服务
	dubboPath = fmt.Sprintf("/dubbo/%s/%s", conf.Service, DubboNodes[PROVIDER])
	this.Lock()
	err = this.client.Create(dubboPath)
	this.Unlock()
	if err != nil {
		log.Error("zkClient.create(path{%s}) = error{%v}", dubboPath, err)
		return err
	}

	params = url.Values{}
	params.Add("interface", conf.Service)
	params.Add("application", this.ApplicationConfig.Name)
	revision = this.ApplicationConfig.Version
	if revision == "" {
		revision = "0.1.0"
	}
	params.Add("revision", revision)
	if conf.Group != "" {
		params.Add("group", conf.Group)
	}
	params.Add("category", (DubboType(CONSUMER)).String())
	params.Add("dubbo", "dubbo-consumer-golang-"+version.Version)
	params.Add("org", this.Organization)
	params.Add("module", this.Module)
	params.Add("owner", this.Owner)
	params.Add("side", (DubboType(CONSUMER)).Role())
	params.Add("pid", processID)
	params.Add("ip", localIp)
	params.Add("timeout", fmt.Sprintf("%v", this.Timeout))
	// params.Add("timestamp", time.Now().Format("20060102150405"))
	params.Add("timestamp", fmt.Sprintf("%d", this.birth))
	if conf.Version != "" {
		params.Add("version", conf.Version)
	}
	// log.Debug("consumer zk url params:%#v", params)
	rawURL = fmt.Sprintf("%s://%s/%s?%s", conf.Protocol, localIp, conf.Service+conf.Version, params.Encode())
	log.Debug("consumer url:%s", rawURL)
	encodedURL = url.QueryEscape(rawURL)
	// log.Debug("url.QueryEscape(consumer url:%s) = %s", rawURL, encodedURL)

	// 把自己注册service consumers里面
	dubboPath = fmt.Sprintf("/dubbo/%s/%s", conf.Service, (DubboType(CONSUMER)).String())
	err = this.registerTempZookeeperNode(dubboPath, encodedURL)
	if err != nil {
		return err
	}

	return nil
}

func (this *consumerZookeeperRegistry) handleZkRestart() {
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
			log.Warn("(consumerZookeeperRegistry)reconnectZkRegistry goroutine exit now...")
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
					log.Warn("(consumerZookeeperRegistry)reconnectZkRegistry goroutine exit now...")
					break LOOP
				case <-time.After(common.TimeSecondDuration(failTimes * registry.REGISTRY_CONN_DELAY)): // 防止疯狂重连zk
				}
				err = this.validateZookeeperClient()
				log.Info("consumerZookeeperRegistry.validateZookeeperClient(zkAddrs{%s}) = error{%#v}", this.client.zkAddrs, err)
				if err == nil {
					// copy this.services
					this.Lock()
					for _, confIf = range this.services {
						services = append(services, confIf)
					}
					this.Unlock()

					flag = true
					for _, confIf = range services {
						err = this.register(confIf.(*registry.ServiceConfig))
						if err != nil {
							log.Error("in (consumerZookeeperRegistry)reRegister, (consumerZookeeperRegistry)register(conf{%#v}) = error{%#v}",
								confIf.(*registry.ServiceConfig), err)
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

func (this *consumerZookeeperRegistry) Watch() (registry.Watcher, error) {
	var (
		ok          bool
		err         error
		dubboPath   string
		client      *zookeeperClient
		iWatcher    registry.Watcher
		zkWatcher   *zookeeperWatcher
		serviceConf *registry.ServiceConfig
	)

	// new client & watcher
	client, err = newZookeeperClient(WatcherZkClient, this.Address, this.RegistryConfig.Timeout)
	if err != nil {
		log.Warn("newZookeeperClient(name:%s, zk addresss{%v}, timeout{%d}) = error{%v}",
			WatcherZkClient, this.Address, this.Timeout, err)
		return nil, err
	}
	iWatcher, err = newZookeeperWatcher(client)
	if err != nil {
		client.Close()
		log.Warn("newZookeeperWatcher() = error{%v}", err)
		return nil, err
	}
	zkWatcher = iWatcher.(*zookeeperWatcher)

	// watch
	this.Lock()
	for _, service := range this.services {
		// 监控相关服务的providers
		if serviceConf, ok = service.(*registry.ServiceConfig); ok {
			dubboPath = fmt.Sprintf("/dubbo/%s/providers", serviceConf.Service)
			log.Info("watch dubbo provider path{%s} and wait to get all provider zk nodes", dubboPath)
			// watchService过程中会产生event，如果zookeeperWatcher{events} channel被塞满，
			// 但是selector还没有准备好接收，下面这个函数就会阻塞，所以起动一个gr以防止阻塞for-loop
			go zkWatcher.watchService(dubboPath, *serviceConf)
		}
	}
	this.Unlock()

	return iWatcher, nil
}

func (this *consumerZookeeperRegistry) GetService(name string) ([]*registry.ServiceURL, error) {
	var (
		ok            bool
		err           error
		dubboPath     string
		nodes         []string
		serviceURL    *registry.ServiceURL
		serviceConfIf registry.ServiceConfigIf
		serviceConf   *registry.ServiceConfig
	)

	ok = false
	this.Lock()
	for k, v := range this.services {
		log.Debug("(consumerZookeeperRegistry)GetService, service{%q}, serviceURL{%#v}", k, v)
	}
	serviceConfIf, ok = this.services[name]
	this.Unlock()
	if !ok {
		return nil, fmt.Errorf("Service{%s} has not been registered", name)
	}
	serviceConf, ok = serviceConfIf.(*registry.ServiceConfig)
	if !ok {
		return nil, fmt.Errorf("Service{%s}: failed to get serviceConfigIf type", name)
	}

	dubboPath = fmt.Sprintf("/dubbo/%s/providers", name)
	err = this.validateZookeeperClient()
	if err != nil {
		return nil, err
	}
	this.Lock()
	nodes, err = this.client.getChildren(dubboPath)
	this.Unlock()
	if err != nil {
		log.Warn("getChildren(dubboPath{%s}) = error{%v}", dubboPath, err)
		return nil, err
	}

	var serviceMap = make(map[string]*registry.ServiceURL)
	for _, n := range nodes {
		serviceURL, err = registry.NewServiceURL(n)
		if err != nil {
			log.Error("NewServiceURL({%s}) = error{%v}", n, err)
			continue
		}
		if !serviceConf.ServiceEqual(serviceURL) {
			log.Warn("serviceURL{%#v} is not compatible with ServiceConfig{%#v}", serviceURL, serviceConf)
			continue
		}

		_, ok := serviceMap[serviceURL.Query.Get(serviceURL.Location)]
		if !ok {
			serviceMap[serviceURL.Location] = serviceURL
			continue
		}
	}

	var services []*registry.ServiceURL
	for _, service := range serviceMap {
		services = append(services, service)
	}

	return services, nil
}

func (this *consumerZookeeperRegistry) ListServices() ([]*registry.ServiceURL, error) {
	var (
		ok            bool
		err           error
		dubboPath     string
		keys          []string
		nodes         []string
		serviceURL    *registry.ServiceURL
		serviceMap    map[string]*registry.ServiceURL
		serviceConfIf registry.ServiceConfigIf
		serviceConf   *registry.ServiceConfig
	)

	err = this.validateZookeeperClient()
	if err != nil {
		return nil, err
	}
	dubboPath = "/" + common.DUBBO
	this.Lock()
	keys, err = this.client.getChildren(dubboPath)
	this.Unlock()
	if err != nil {
		log.Warn("getChildren(dubboPath{%s}) = error{%v}", dubboPath, err)
		return nil, err
	}

	serviceMap = make(map[string]*registry.ServiceURL)
	for _, key := range keys {
		ok = false
		this.Lock()
		serviceConfIf, ok = this.services[key]
		this.Unlock()
		if !ok {
			log.Warn("Service{%s} has not been registered", key)
			continue
		}
		serviceConf, ok = serviceConfIf.(*registry.ServiceConfig)
		if !ok {
			continue
		}

		dubboPath = fmt.Sprintf("/dubbo/%s/providers", key)
		this.Lock()
		nodes, err = this.client.getChildren(dubboPath)
		this.Unlock()
		if err != nil {
			log.Warn("getChildren(dubboPath{%s}) = error{%v}", dubboPath, err)
			continue
		}

		for _, n := range nodes {
			serviceURL, err = registry.NewServiceURL(n)
			if err != nil {
				log.Error("NewServiceURL({%s}) = error{%v}", n, err)
				continue
			}
			if !serviceConf.ServiceEqual(serviceURL) {
				log.Warn("serviceURL{%#v} is not compatible with ServiceConfig{%#v}", serviceURL, serviceConf)
				continue
			}

			_, ok := serviceMap[serviceURL.Query.Get(serviceURL.Location)]
			if !ok {
				serviceMap[serviceURL.Location] = serviceURL
				continue
			}
		}
	}

	var services []*registry.ServiceURL
	for _, service := range serviceMap {
		services = append(services, service)
	}

	return services, nil
}

func (this *consumerZookeeperRegistry) String() string {
	return "dubbogo rpc consumer zookeeper registry"
}

// 删除zk上注册的registers
func (this *consumerZookeeperRegistry) closeRegisters() {
	var (
		key string
		// err error
	)

	this.Lock()
	log.Info("begin to close consumer zk client")
	// 先关闭旧client，以关闭tmp node
	this.client.Close()
	this.client = nil
	// for key = range this.registers {
	// 	// log.Info("begin to delete key:%v", key)
	// 	// err = this.client.Delete(key)
	// 	log.Debug("delete register consumer zk path:%s, err = %v\n", key, err)
	// 	// delete(this.registers, key)
	// }
	// this.registers = nil
	for key = range this.services {
		// 	delete(this.services, key)
		log.Debug("delete register consumer zk path:%s", key)
	}
	this.services = nil
	this.Unlock()
}

func (this *consumerZookeeperRegistry) Close() {
	close(this.done)
	this.wg.Wait()
	this.closeRegisters()
}

// Copyright 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of c source code is
// governed by Apache License 2.0.

// Package gxzookeeper provides a zookeeper driver based on samuel/go-zookeeper/zk
package gxzookeeper

import (
	"path"
	"strings"
	"sync"
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
	jerrors "github.com/juju/errors"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	ErrZkClientConnNil = jerrors.Errorf("zookeeperclient{conn} is nil")
)

type ZookeeperClient struct {
	name          string
	zkAddrs       []string
	sync.Mutex             // for conn
	conn          *zk.Conn // 这个conn不能被close两次，否则会收到 “panic: close of closed channel”
	done          chan struct{}
	wait          sync.WaitGroup
	eventRegistry map[string][]*chan struct{}
}

func stateToString(state zk.State) string {
	switch state {
	case zk.StateDisconnected:
		return "zookeeper disconnected"
	case zk.StateConnecting:
		return "zookeeper connecting"
	case zk.StateAuthFailed:
		return "zookeeper auth failed"
	case zk.StateConnectedReadOnly:
		return "zookeeper connect readonly"
	case zk.StateSaslAuthenticated:
		return "zookeeper sasl authenticaed"
	case zk.StateExpired:
		return "zookeeper connection expired"
	case zk.StateConnected:
		return "zookeeper conneced"
	case zk.StateHasSession:
		return "zookeeper has session"
	case zk.StateUnknown:
		return "zookeeper unknown state"
	case zk.State(zk.EventNodeDeleted):
		return "zookeeper node deleted"
	case zk.State(zk.EventNodeDataChanged):
		return "zookeeper node data changed"
	default:
		return state.String()
	}

	return "zookeeper unknown state"
}

func NewClient(name string, zkAddrs []string, connTimeout time.Duration) (*ZookeeperClient, error) {
	var (
		err   error
		event <-chan zk.Event
		c     *ZookeeperClient
	)

	c = &ZookeeperClient{
		name:          name,
		zkAddrs:       zkAddrs,
		done:          make(chan struct{}),
		eventRegistry: make(map[string][]*chan struct{}),
	}
	// connect to zookeeper
	c.conn, event, err = zk.Connect(zkAddrs, connTimeout)
	if err != nil {
		return nil, err
	}

	c.wait.Add(1)
	go c.handleZkEvent(event)

	return c, nil
}

func (c *ZookeeperClient) handleZkEvent(session <-chan zk.Event) {
	var (
		state int
		event zk.Event
	)

	defer func() {
		c.wait.Done()
		log.Info("zk{path:%v, name:%s} connection goroutine game over.", c.zkAddrs, c.name)
	}()

LOOP:
	for {
		select {
		case <-c.done:
			break LOOP

		case event = <-session:
			log.Warn("client{%s} get a zookeeper event{type:%s, server:%s, path:%s, state:%d-%s, err:%s}",
				c.name, event.Type.String(), event.Server, event.Path, event.State, stateToString(event.State), event.Err)
			switch (int)(event.State) {
			case (int)(zk.StateDisconnected):
				log.Warn("zk{addr:%s} state is StateDisconnected, so close the zk client{name:%s}.", c.zkAddrs, c.name)
				c.Stop()
				c.Lock()
				if c.conn != nil {
					c.conn.Close()
					c.conn = nil
				}
				c.Unlock()
				break LOOP

			case (int)(zk.EventNodeDataChanged), (int)(zk.EventNodeChildrenChanged):
				log.Info("zkClient{%s} get zk node changed event{path:%s}", c.name, event.Path)
				c.Lock()
				for p, a := range c.eventRegistry {
					if strings.HasPrefix(p, event.Path) {
						log.Info("send event{state:zk.EventNodeDataChange, Path:%s} notify event to path{%s} related watcher", event.Path, p)
						for _, e := range a {
							*e <- struct{}{}
						}
					}
				}
				c.Unlock()

			case (int)(zk.StateConnecting), (int)(zk.StateConnected), (int)(zk.StateHasSession):
				if state != (int)(zk.StateConnecting) || state != (int)(zk.StateDisconnected) {
					continue
				}
				if a, ok := c.eventRegistry[event.Path]; ok && 0 < len(a) {
					for _, e := range a {
						*e <- struct{}{}
					}
				}
			}
			state = (int)(event.State)
		}
	}
}

func (c *ZookeeperClient) RegisterEvent(zkPath string, event *chan struct{}) {
	if zkPath == "" || event == nil {
		return
	}

	c.Lock()
	a := c.eventRegistry[zkPath]
	a = append(a, event)
	c.eventRegistry[zkPath] = a
	c.Unlock()
	log.Debug("zkClient{%s} register event{path:%s, ptr:%p}", c.name, zkPath, event)
}

func (c *ZookeeperClient) UnregisterEvent(zkPath string, event *chan struct{}) {
	if zkPath == "" {
		return
	}

	c.Lock()
	for {
		a, ok := c.eventRegistry[zkPath]
		if !ok {
			break
		}
		for i, e := range a {
			if e == event {
				arr := a
				a = append(arr[:i], arr[i+1:]...)
				log.Debug("zkClient{%s} unregister event{path:%s, event:%p}", c.name, zkPath, event)
			}
		}
		log.Debug("after zkClient{%s} unregister event{path:%s, event:%p}, array length %d", c.name, zkPath, event, len(a))
		if len(a) == 0 {
			delete(c.eventRegistry, zkPath)
		} else {
			c.eventRegistry[zkPath] = a
		}
		break
	}
	c.Unlock()
}

func (c *ZookeeperClient) Done() <-chan struct{} {
	return c.done
}

func (c *ZookeeperClient) Stop() bool {
	select {
	case <-c.done:
		return true
	default:
		close(c.done)
	}

	return false
}

func (c *ZookeeperClient) ValidateConnection() bool {
	select {
	case <-c.done:
		return false
	default:
	}

	var valid bool = true
	c.Lock()
	if c.conn == nil {
		valid = false
	}
	c.Unlock()

	return valid
}

func (c *ZookeeperClient) Close() {
	c.Stop()
	c.wait.Wait()
	c.Lock()
	if c.conn != nil {
		c.conn.Close() // 等着所有的goroutine退出后，再关闭连接
		c.conn = nil
	}
	c.Unlock()
	log.Warn("zkClient{name:%s, zk addr:%s} done now.", c.name, c.zkAddrs)
}

// 节点须逐级创建
func (c *ZookeeperClient) CreateZkPath(basePath string) error {
	var (
		err     error
		tmpPath string
	)

	log.Debug("ZookeeperClient.Create(basePath{%s})", basePath)
	for _, str := range strings.Split(basePath, "/")[1:] {
		tmpPath = path.Join(tmpPath, "/", str)
		// log.Debug("create zookeeper path: \"%s\"\n", tmpPath)
		err = ErrZkClientConnNil
		c.Lock()
		if c.conn != nil {
			_, err = c.conn.Create(tmpPath, []byte(""), 0, zk.WorldACL(zk.PermAll))
		}
		c.Unlock()
		if err != nil {
			if err == zk.ErrNodeExists {
				log.Error("zk.create(\"%s\") exists\n", tmpPath)
			} else {
				log.Error("zk.create(\"%s\") error(%v)\n", tmpPath, err)
				return err
			}
		}
	}

	return nil
}

// 像创建一样，删除节点的时候也只能从叶子节点逐级回退删除
// 当节点还有子节点的时候，删除是不会成功的
func (c *ZookeeperClient) DeleteZkPath(basePath string) error {
	var (
		err error
	)

	err = ErrZkClientConnNil
	c.Lock()
	if c.conn != nil {
		err = c.conn.Delete(basePath, -1)
	}
	c.Unlock()

	return err
}

func (c *ZookeeperClient) RegisterTemp(basePath string, node string) (string, error) {
	var (
		err     error
		data    []byte
		zkPath  string
		tmpPath string
	)

	err = ErrZkClientConnNil
	data = []byte("")
	zkPath = path.Join(basePath) + "/" + node
	c.Lock()
	if c.conn != nil {
		tmpPath, err = c.conn.Create(zkPath, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	}
	c.Unlock()
	// log.Debug("ZookeeperClient.RegisterTemp(basePath{%s}) = tempPath{%s}", zkPath, tmpPath)
	if err != nil {
		log.Error("conn.Create(\"%s\", zk.FlagEphemeral) = error(%v)\n", zkPath, err)
		// if err != zk.ErrNodeExists {
		return "", err
		// }
	}
	log.Debug("zkClient{%s} create a temp zookeeper node:%s\n", c.name, tmpPath)
	return tmpPath, nil
}

func (c *ZookeeperClient) RegisterTempSeq(basePath string, data []byte) (string, error) {
	var (
		err     error
		tmpPath string
	)

	err = ErrZkClientConnNil
	c.Lock()
	if c.conn != nil {
		tmpPath, err = c.conn.Create(path.Join(basePath)+"/", data, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	}
	c.Unlock()
	log.Debug("ZookeeperClient.RegisterTempSeq(basePath{%s}) = tempPath{%s}", basePath, tmpPath)
	if err != nil {
		log.Error("zkClient{%s} conn.Create(\"%s\", \"%s\", zk.FlagEphemeral|zk.FlagSequence) error(%v)\n",
			c.name, basePath, string(data), err)
		// if err != zk.ErrNodeExists {
		return "", err
		// }
	}
	log.Debug("zkClient{%s} create a temp zookeeper node:%s\n", c.name, tmpPath)
	return tmpPath, nil
}

func (c *ZookeeperClient) GetChildrenW(path string) ([]string, <-chan zk.Event, error) {
	var (
		err      error
		children []string
		stat     *zk.Stat
		watch    <-chan zk.Event
	)

	err = ErrZkClientConnNil
	c.Lock()
	if c.conn != nil {
		children, stat, watch, err = c.conn.ChildrenW(path)
	}
	c.Unlock()
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, nil, jerrors.Errorf("path{%s} has none children", path)
		}
		log.Error("zk.ChildrenW(path{%s}) = error(%v)", path, err)
		return nil, nil, err
	}
	if stat == nil {
		return nil, nil, jerrors.Errorf("path{%s} has none children", path)
	}
	if len(children) == 0 {
		return nil, nil, jerrors.Errorf("path{%s} has none children", path)
	}

	return children, watch, nil
}

func (c *ZookeeperClient) GetChildren(path string) ([]string, error) {
	var (
		err      error
		children []string
		stat     *zk.Stat
	)

	err = ErrZkClientConnNil
	c.Lock()
	if c.conn != nil {
		children, stat, err = c.conn.Children(path)
	}
	c.Unlock()
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, jerrors.Errorf("path{%s} has none children", path)
		}
		log.Error("zk.Children(path{%s}) = error(%v)", path, err)
		return nil, err
	}
	if stat == nil {
		return nil, jerrors.Errorf("path{%s} has none children", path)
	}
	if len(children) == 0 {
		return nil, jerrors.Errorf("path{%s} has none children", path)
	}

	return children, nil
}

func (c *ZookeeperClient) ExistW(zkPath string) (<-chan zk.Event, error) {
	var (
		exist bool
		err   error
		watch <-chan zk.Event
	)

	err = ErrZkClientConnNil
	c.Lock()
	if c.conn != nil {
		exist, _, watch, err = c.conn.ExistsW(zkPath)
	}
	c.Unlock()
	if err != nil {
		log.Error("zkClient{%s}.ExistsW(path{%s}) = error{%v}.", c.name, zkPath, err)
		return nil, err
	}
	if !exist {
		log.Warn("zkClient{%s}'s App zk path{%s} does not exist.", c.name, zkPath)
		return nil, jerrors.Errorf("zkClient{%s} App zk path{%s} does not exist.", c.name, zkPath)
	}

	return watch, nil
}

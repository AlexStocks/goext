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
	ErrZkClientConnNil = jerrors.Errorf("client{conn} is nil")
)

type Client struct {
	zkAddrs       []string
	sync.Mutex             // for conn
	conn          *zk.Conn // 这个conn不能被close两次，否则会收到 “panic: close of closed channel”
	done          chan struct{}
	wait          sync.WaitGroup
	eventRegistry map[string][]*chan struct{}
}

func StateToString(state zk.State) string {
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

func NewClient(zkAddrs []string, connTimeout time.Duration) (*Client, error) {
	var (
		err   error
		c     *Client
		event <-chan zk.Event
	)

	c = &Client{
		zkAddrs:       zkAddrs,
		done:          make(chan struct{}),
		eventRegistry: make(map[string][]*chan struct{}),
	}
	// connect to zookeeper
	c.conn, event, err = zk.Connect(zkAddrs, connTimeout)
	if err != nil {
		return nil, jerrors.Annotatef(err, "zk.Connect(zk addr:%#v, timeout:%d)", zkAddrs, connTimeout)
	}

	c.wait.Add(1)
	go c.handleZkEvent(event)

	return c, nil
}

func (c *Client) handleZkEvent(session <-chan zk.Event) {
	var (
		state int
		event zk.Event
	)

	defer func() {
		c.wait.Done()
		log.Info("zk{path:%v} connection goroutine game over.", c.zkAddrs)
	}()

LOOP:
	for {
		select {
		case <-c.done:
			break LOOP

		case event = <-session:
			log.Warn("client get a zookeeper event{type:%s, server:%s, path:%s, state:%d-%s, err:%s}",
				event.Type.String(), event.Server, event.Path, event.State, StateToString(event.State), event.Err)
			switch (int)(event.State) {
			case (int)(zk.StateDisconnected):
				log.Warn("zk{addr:%#v} state is StateDisconnected, so close the zk client.", c.zkAddrs)
				c.Stop()
				c.Lock()
				if c.conn != nil {
					c.conn.Close()
					c.conn = nil
				}
				c.Unlock()
				break LOOP

			case (int)(zk.EventNodeDataChanged), (int)(zk.EventNodeChildrenChanged):
				log.Info("zkClient get zk node changed event{path:%s}", event.Path)
				c.Lock()
				for p, a := range c.eventRegistry {
					if strings.HasPrefix(p, event.Path) {
						log.Info("send event{zk.EventNodeDataChange, zk.Path:%s} to path{%s} related watcher", event.Path, p)
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

func (c *Client) RegisterEvent(path string, event *chan struct{}) {
	if path == "" || event == nil {
		return
	}

	c.Lock()
	a := c.eventRegistry[path]
	a = append(a, event)
	c.eventRegistry[path] = a
	c.Unlock()
	log.Debug("zkClient register event{path:%s, ptr:%p}", path, event)
}

func (c *Client) UnregisterEvent(path string, event *chan struct{}) {
	if path == "" {
		return
	}

	c.Lock()
	for {
		a, ok := c.eventRegistry[path]
		if !ok {
			break
		}
		for i, e := range a {
			if e == event {
				arr := a
				a = append(arr[:i], arr[i+1:]...)
				log.Debug("zkClient unregister event{path:%s, event:%p}", path, event)
			}
		}
		log.Debug("after zkClient unregister event{path:%s, event:%p}, array length %d", path, event, len(a))
		if len(a) == 0 {
			delete(c.eventRegistry, path)
		} else {
			c.eventRegistry[path] = a
		}
		break
	}
	c.Unlock()
}

func (c *Client) Done() <-chan struct{} {
	return c.done
}

func (c *Client) Stop() bool {
	select {
	case <-c.done:
		return true
	default:
		close(c.done)
	}

	return false
}

func (c *Client) ValidateConnection() bool {
	select {
	case <-c.done:
		return false
	default:
	}

	var valid = true
	c.Lock()
	if c.conn == nil {
		valid = false
	}
	c.Unlock()

	return valid
}

func (c *Client) Close() {
	c.Stop()
	c.wait.Wait()
	c.Lock()
	if c.conn != nil {
		c.conn.Close() // 等着所有的goroutine退出后，再关闭连接
		c.conn = nil
	}
	c.Unlock()
	log.Warn("zkClient{zk addr:%s} done now.", c.zkAddrs)
}

// 节点须逐级创建
func (c *Client) CreateZkPath(basePath string) error {
	var (
		err     error
		tmpPath string
	)

	log.Debug("Client.Create(basePath{%s})", basePath)
	for _, str := range strings.Split(basePath, "/")[1:] {
		tmpPath = path.Join(tmpPath, "/", str)
		err = ErrZkClientConnNil
		c.Lock()
		if c.conn != nil {
			_, err = c.conn.Create(tmpPath, []byte(""), 0, zk.WorldACL(zk.PermAll))
		}
		c.Unlock()
		if err != nil {
			if err != zk.ErrNodeExists {
				return jerrors.Annotatef(err, "zk.Create(path:%s)", tmpPath)
			}
		}
	}

	return nil
}

// 像创建一样，删除节点的时候也只能从叶子节点逐级回退删除
// 当节点还有子节点的时候，删除是不会成功的
func (c *Client) DeleteZkPath(basePath string) error {
	err := ErrZkClientConnNil
	c.Lock()
	defer c.Unlock()
	if c.conn != nil {
		err = c.conn.Delete(basePath, -1)
		if err != nil {
			return jerrors.Annotatef(err, "zk.Delete(path:%s)", basePath)
		}
	}

	return nil
}

func (c *Client) RegisterTemp(path string, data []byte) (string, error) {
	var (
		err     error
		tmpPath string
	)

	err = ErrZkClientConnNil
	c.Lock()
	if c.conn != nil {
		tmpPath, err = c.conn.Create(path, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	}
	c.Unlock()
	if err != nil {
		// if err != zk.ErrNodeExists {
		return "", jerrors.Annotatef(err, "zk.Create(%s, ephemeral)", path)
		// }
	}

	return tmpPath, nil
}

func (c *Client) RegisterTempSeq(path string, data []byte) (string, error) {
	var (
		err     error
		tmpPath string
	)

	err = ErrZkClientConnNil
	c.Lock()
	if c.conn != nil {
		tmpPath, err = c.conn.Create(path, data, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	}
	c.Unlock()
	if err != nil {
		// if err != zk.ErrNodeExists {
		return "", jerrors.Annotatef(err, "zk.Create(%s, sequence | ephemeral)", path)
		// }
	}

	return tmpPath, nil
}

func (c *Client) GetChildrenW(path string) ([]string, <-chan zk.Event, error) {
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
		return nil, nil, jerrors.Annotatef(err, "zk.ChildrenW(%s)", path)
	}
	if stat == nil {
		return nil, nil, jerrors.Errorf("path{%s} has none children", path)
	}
	if len(children) == 0 {
		return nil, nil, jerrors.Errorf("path{%s} has none children", path)
	}

	return children, watch, nil
}

func (c *Client) Get(path string) ([]byte, error) {
	var (
		err  error
		data []byte
		stat *zk.Stat
	)

	err = ErrZkClientConnNil
	c.Lock()
	if c.conn != nil {
		data, stat, err = c.conn.Get(path)
	}
	c.Unlock()
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, jerrors.Errorf("path{%s} has none children", path)
		}
		return nil, jerrors.Annotatef(err, "zk.Children(path:%s)", path)
	}
	if stat == nil {
		return nil, jerrors.Errorf("path{%s} has none children", path)
	}
	if len(data) == 0 {
		return nil, jerrors.Errorf("path{%s} has none children", path)
	}

	return data, nil
}

func (c *Client) GetChildren(path string) ([]string, error) {
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
		return nil, jerrors.Annotatef(err, "zk.Children(path:%s)", path)
	}
	if stat == nil || stat.NumChildren == 0 {
		return nil, jerrors.Errorf("path{%s} has none children", path)
	}
	if len(children) == 0 {
		return nil, jerrors.Errorf("path{%s} has none children", path)
	}

	return children, nil
}

func (c *Client) ExistW(path string) (<-chan zk.Event, error) {
	var (
		exist bool
		err   error
		watch <-chan zk.Event
	)

	err = ErrZkClientConnNil
	c.Lock()
	if c.conn != nil {
		exist, _, watch, err = c.conn.ExistsW(path)
	}
	c.Unlock()
	if err != nil {
		return nil, jerrors.Annotatef(err, "zk.ExistsW(path:%s)", path)
	}
	if !exist {
		return nil, jerrors.Errorf("zkClient App zk path{%s} does not exist.", path)
	}

	return watch, nil
}

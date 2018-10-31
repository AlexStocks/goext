// Copyright 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of c source code is
// governed by Apache License 2.0.

// Package gxzookeeper provides a zookeeper driver based on samuel/go-zookeeper/zk
package gxzookeeper

import (
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

import (
	"github.com/AlexStocks/goext/runtime"
	log "github.com/AlexStocks/log4go"
	jerrors "github.com/juju/errors"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	zkLockPrefix = "lock-"
)

var (
	// ErrDeadlock is returned by Lock when trying to lock twice without unlocking first
	ErrDeadlock = jerrors.New("zk: trying to acquire a lock twice")
	// ErrNotLocked is returned by Unlock when trying to release a lock that has not first be acquired.
	ErrNotLocked = jerrors.New("zk: not locked")
)

type Client struct {
	conn      *zk.Conn // 这个conn不能被close两次，否则会收到 “panic: close of closed channel”
	mutex     sync.Mutex
	leaderMap map[string]string
	lockMap   map[string]*zk.Lock
}

func NewClient(conn *zk.Conn) *Client {
	return &Client{
		conn:      conn,
		leaderMap: make(map[string]string),
		lockMap:   make(map[string]*zk.Lock),
	}
}

func (c *Client) ZkConn() *zk.Conn {
	return c.conn
}

func (c *Client) StateToString(state zk.State) string {
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

// 节点须逐级创建
func (c *Client) CreateZkPath(basePath string) error {
	var (
		err     error
		tmpPath string
	)

	if strings.HasSuffix(basePath, "/") {
		basePath = strings.TrimSuffix(basePath, "/")
	}

	for _, str := range strings.Split(basePath, "/")[1:] {
		tmpPath = path.Join(tmpPath, "/", str)
		_, err = c.conn.Create(tmpPath, []byte(""), 0, zk.WorldACL(zk.PermAll))
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
func (c *Client) DeleteZkPath(path string) error {
	if strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}

	err := c.conn.Delete(path, -1)
	if err != nil {
		return jerrors.Annotatef(err, "zk.Delete(path:%s)", path)
	}

	return nil
}

func (c *Client) RegisterTemp(path string, data []byte) (string, error) {
	var (
		err     error
		tmpPath string
	)

	if strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}

	tmpPath, err = c.conn.Create(path, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		return "", jerrors.Annotatef(err, "zk.Create(%s, ephemeral)", path)
	}

	return tmpPath, nil
}

func (c *Client) RegisterTempSeq(path string, data []byte) (string, error) {
	var (
		err     error
		tmpPath string
	)

	if strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}

	tmpPath, err = c.conn.Create(path, data, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		return "", jerrors.Annotatef(err, "zk.Create(%s, sequence | ephemeral)", path)
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

	if strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}

	children, stat, watch, err = c.conn.ChildrenW(path)
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

	if strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}

	data, stat, err = c.conn.Get(path)
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

	if strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}

	children, stat, err = c.conn.Children(path)
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

func (c *Client) Exist(path string) (bool, error) {
	var (
		exist bool
		err   error
	)

	if strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}

	exist, _, err = c.conn.Exists(path)
	return exist, jerrors.Trace(err)
}

func (c *Client) ExistW(path string) (<-chan zk.Event, error) {
	var (
		exist bool
		err   error
		watch <-chan zk.Event
	)

	if strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}

	exist, _, watch, err = c.conn.ExistsW(path)
	if err != nil {
		return nil, jerrors.Annotatef(err, "zk.ExistsW(path:%s)", path)
	}
	if !exist {
		return nil, jerrors.Errorf("zkClient App zk path{%s} does not exist.", path)
	}

	return watch, nil
}

func getSequenceNumber(path, prefix string) (int, error) {
	str := strings.TrimPrefix(path, prefix)
	seq, err := strconv.Atoi(str)
	if err != nil {
		return 0, jerrors.Trace(err)
	}

	return seq, nil
}

// get the minimum sequence number.
func GetMinSequenceNumber(children []string, prefix string) (seq int, index int, retError error) {
	if len(children) == 0 {
		retError = jerrors.New("@children is nil")
		return
	}

	var err error
	index = 0
	seq, err = getSequenceNumber(children[0], prefix)
	if err != nil {
		retError = jerrors.Trace(err)
		return
	}

	for i := 1; i < len(children); i++ {
		num, err := getSequenceNumber(children[i], prefix)
		if err != nil {
			retError = jerrors.Trace(err)
			return
		}

		if num < seq {
			seq = num
			index = i
		}
	}

	return
}

// get the maximum sequence number.
func GetMaxSequenceNumber(children []string, prefix string) (seq int, index int, retError error) {
	if len(children) == 0 {
		retError = jerrors.New("@children is nil")
		return
	}

	var err error
	index = 0
	seq, err = getSequenceNumber(children[0], prefix)
	if err != nil {
		retError = jerrors.Trace(err)
		return
	}

	for i := 1; i < len(children); i++ {
		num, err := getSequenceNumber(children[i], prefix)
		if err != nil {
			retError = jerrors.Trace(err)
			return
		}

		if num > seq {
			seq = num
			index = i
		}
	}

	return
}

func (c *Client) GetMinZkPath(baseZkPath, prefix string) ([]string, string, error) {
	children, err := c.GetChildren(baseZkPath)
	if err != nil {
		return nil, "", jerrors.Trace(err)
	}
	_, index, err := GetMinSequenceNumber(children, prefix)
	if err != nil {
		return nil, "", jerrors.Trace(err)
	}

	return children, baseZkPath + "/" + children[index], nil
}

func getLockPrefixPath(basePath, prefix, zkLockPath string, siblings []string) (string, error) {
	// path := basePath + "/" + prefix
	path := basePath + "/" + zkLockPrefix
	seq, err := getSequenceNumber(zkLockPath, path)
	if err != nil {
		return "", jerrors.Trace(err)
	}
	if seq == -1 {
		return "", jerrors.New("can not get legal numeric digit")
	}

	sort.Slice(siblings, func(i, j int) bool {
		seqI, _ := getSequenceNumber(siblings[i], prefix)
		seqJ, _ := getSequenceNumber(siblings[j], prefix)

		return seqI < seqJ
	})

	pos := -1
	for idx := range siblings {
		if basePath+"/"+siblings[idx] == zkLockPath {
			pos = idx
			break
		}
	}

	if 0 < pos {
		return siblings[pos-1], nil
	}

	log.Debug("basePath:%s, prefix:%s, zkLockPath:%s, siblings:%v", basePath, prefix, zkLockPath, siblings)
	return "", jerrors.New("illegal pos " + strconv.Itoa(pos-1))
}

func checkOutTimeOut(data []byte, timeout time.Duration) bool {
	nowUnixTime := time.Now().Unix()
	//get the znode create time
	createUnixTime, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return true
	}
	timeoutUnixTime := int64(createUnixTime) + int64(timeout.Seconds())

	log.Debug("gr:%d, zk time:%d, timeout:%d %d, now:%d",
		gxruntime.GoID(), createUnixTime, timeout.Seconds(), timeoutUnixTime, nowUnixTime)
	return timeoutUnixTime <= nowUnixTime
}

// string is the lock path
// when error is not nil, the lock is failed.
func (c *Client) lock(basePath, lockPrefix string, timeout time.Duration) (string, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if strings.HasSuffix(basePath, "/") {
		basePath = strings.TrimSuffix(basePath, "/")
	}

	err := c.CreateZkPath(basePath)
	if err != nil {
		return "", jerrors.Trace(err)
	}

	// create seq/tmp zk path, its data value is current unix time.
	zkPrefixPath := basePath + "/" + lockPrefix
	zkData := []byte(strconv.FormatInt(time.Now().Unix(), 10))
	zkLockPath, err := c.RegisterTempSeq(zkPrefixPath, zkData)
	if err != nil {
		return zkLockPath, jerrors.Trace(err)
	}
	log.Debug("gr:%d, lock path:%s, lock data:%s", gxruntime.GoID(), zkLockPath, string(zkData))

	// children, minSequencePath, err := c.GetMinZkPath(basePath, lockPrefix)
	children, minSequencePath, err := c.GetMinZkPath(basePath, zkLockPrefix)
	if err != nil {
		return zkLockPath, jerrors.Trace(err)
	}
	// the created lock path is the minimum znode.
	if minSequencePath == zkLockPath {
		return zkLockPath, nil
	}

	// if the created znode is not the minimum znode,
	// watch the last znode delete notification
	prePath, err := getLockPrefixPath(basePath, lockPrefix, zkLockPath, children)
	if err != nil {
		return zkLockPath, jerrors.Trace(err)
	}
	existFlag, _, w, err := c.conn.ExistsW(prePath)
	if !existFlag || err != nil {
		// recheck the minimum zookeeper path in case of the leaky zookeeper notification
		// _, minSequencePath, err := c.GetMinZkPath(basePath, lockPrefix)
		_, minSequencePath, err := c.GetMinZkPath(basePath, zkLockPrefix)
		if err != nil {
			return zkLockPath, jerrors.Trace(err)
		}
		if minSequencePath == zkLockPath {
			return zkLockPath, nil
		}
		// return zkLockPath, jerrors.Errorf("c.ExistW(path:%s) = flag:%v, err:%s", prePath, existFlag, err)

		// timeout /= 15
		// if timeout < 1e6 {
		// 	timeout = 1e6
		// }
	}
	select {
	case event := <-w:
		if event.Type == zk.EventNodeDeleted {
			exist, err := c.Exist(basePath)
			if err != nil {
				return zkLockPath, jerrors.Trace(err)
			}
			if exist {
				// _, minSequencePath, err = c.GetMinZkPath(basePath, lockPrefix)
				_, minSequencePath, err = c.GetMinZkPath(basePath, zkLockPrefix)
				if err != nil {
					return zkLockPath, jerrors.Trace(err)
				}

				if minSequencePath == zkLockPath {
					return zkLockPath, nil
				}
			}
		}
	case <-time.After(timeout):
		// timeout, delete its znode
		children, err := c.GetChildren(basePath)
		if err != nil {
			return zkLockPath, jerrors.Trace(err)
		}

		// delete timeout zookeeper path
		for idx := range children {
			timeoutFlag := false
			zkTimeoutPath := basePath + "/" + children[idx]
			if zkTimeoutPath == zkLockPath {
				timeoutFlag = true
			} else {
				data, err := c.Get(zkTimeoutPath)
				if err != nil {
					log.Warn("gr:%d, c.Get(%s) = error:%s", gxruntime.GoID(), zkTimeoutPath, jerrors.Trace(err))
					continue
				}
				timeoutFlag = checkOutTimeOut(data, timeout)
			}

			if timeoutFlag {
				err = c.DeleteZkPath(zkTimeoutPath)
				log.Debug("gr:%d, c.DeleteZkPath(%s) = error:%s", gxruntime.GoID(), zkTimeoutPath)
			}
		}
	}

	return "", jerrors.New("lock timeout")
}

// if @timeout <= 0, Compaign will loop to get the leadership until success.
func (c *Client) Compaign(basePath string, timeout time.Duration) error {
	var (
		grID            int
		flag            bool
		err             error
		path            string
		leaderKey       string
		lockPrefix      string
		timeoutInterval time.Duration
	)

	grID = gxruntime.GoID()
	leaderKey = basePath + strconv.Itoa(grID)

	c.mutex.Lock()
	_, flag = c.leaderMap[leaderKey]
	c.mutex.Unlock()
	if flag {
		return jerrors.Annotatef(ErrDeadlock, "gr:%d", grID)
	}

	timeoutInterval = timeout
	if timeoutInterval <= 0 {
		timeoutInterval = 1e6
	}
	lockPrefix = zkLockPrefix + "1"
	if timeout <= 0 {
		lockPrefix = zkLockPrefix + "0"
	}

	for {
		path, err = c.lock(basePath, lockPrefix, timeout)
		log.Debug("gr:%d, timeout%d, path:%s, error:%s",
			gxruntime.GoID(), timeout, path, jerrors.ErrorStack(err))
		if err == nil {
			break
		}
		if timeout > 0 {
			break
		}
	}

	if err == nil {
		c.mutex.Lock()
		c.leaderMap[leaderKey] = path
		c.mutex.Unlock()
	}

	return jerrors.Trace(err)
}

func (c *Client) Resign(basePath string) error {
	grID := gxruntime.GoID()
	leaderKey := basePath + strconv.Itoa(grID)
	c.mutex.Lock()
	path, flag := c.leaderMap[leaderKey]
	c.mutex.Unlock()
	if !flag {
		return jerrors.Annotatef(ErrNotLocked, "gr:%d", grID)
	}

	log.Debug("gr:%d, unlock path:%s", grID, path)
	return jerrors.Trace(c.DeleteZkPath(path))
}

func (c *Client) Lock(basePath string) error {
	if strings.HasSuffix(basePath, "/") {
		basePath = strings.TrimSuffix(basePath, "/")
	}

	grID := gxruntime.GoID()
	leaderKey := basePath + strconv.Itoa(grID)

	c.mutex.Lock()
	lock, flag := c.lockMap[leaderKey]
	c.mutex.Unlock()
	if !flag {
		lock = zk.NewLock(c.conn, basePath, zk.WorldACL(zk.PermAll))
	}

	err := lock.Lock()
	if err == nil && !flag {
		c.mutex.Lock()
		c.lockMap[leaderKey] = lock
		c.mutex.Unlock()
	}

	return jerrors.Trace(err)
}

func (c *Client) Unlock(basePath string) error {
	if strings.HasSuffix(basePath, "/") {
		basePath = strings.TrimSuffix(basePath, "/")
	}

	grID := gxruntime.GoID()
	leaderKey := basePath + strconv.Itoa(grID)

	c.mutex.Lock()
	lock, flag := c.lockMap[leaderKey]
	c.mutex.Unlock()
	if !flag {
		return jerrors.Trace(ErrNotLocked)
	}

	return jerrors.Trace(lock.Unlock())
}

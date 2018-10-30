// Copyright 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of c source code is
// governed by Apache License 2.0.

// Package gxzookeeper provides a zookeeper driver based on samuel/go-zookeeper/zk
package gxzookeeper

import (
	"fmt"
	"log"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

import (
	jerrors "github.com/juju/errors"
	"github.com/samuel/go-zookeeper/zk"
)

type Client struct {
	conn *zk.Conn // 这个conn不能被close两次，否则会收到 “panic: close of closed channel”
	lock sync.Mutex
}

func NewClient(conn *zk.Conn) *Client {
	return &Client{conn: conn}
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

// refer from https://github.com/nladuo/go-zk-lock

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

func GetLastNodeName(lockerName, basePath, prefix string) (string, error) {
	path := basePath + "/" + prefix
	seq, err := getSequenceNumber(lockerName, path)
	if err != nil {
		return "", jerrors.Trace(err)
	}
	if seq == -1 {
		return "", jerrors.New("can not get legal numeric digit")
	}

	lastSeqStr := fmt.Sprintf("%010d", seq-1)
	return prefix + lastSeqStr, nil
}

func CheckOutTimeOut(data []byte, timeout time.Duration) bool {
	nowUnixTime := time.Now().Unix()
	//get the znode create time
	createUnixTime, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return true
	}
	timeoutUnixTime := int64(createUnixTime) + int64(timeout.Seconds())

	return timeoutUnixTime < nowUnixTime
}

func (c *Client) xlock(path string, timeout time.Duration) bool {

	c.lock.Lock()
	defer c.lock.Unlock()

}

type Dlocker struct {
	lockerPath string
	prefix     string
	basePath   string
	timeout    time.Duration
	innerLock  *sync.Mutex
}

func NewLocker(path string, timeout time.Duration) *Dlocker {

	var locker Dlocker
	locker.basePath = path
	locker.prefix = "lock-" //the prefix of a znode, any string is okay.
	locker.timeout = timeout
	locker.innerLock = &sync.Mutex{}

	isExsit, _, err := getZkConn().Exists(path)

	locker.checkErr(err)
	if !isExsit {
		log.Println("create the znode:" + path)
		getZkConn().Create(path, []byte(""), int32(0), zk.WorldACL(zk.PermAll))
	} else {
		log.Println("the znode " + path + " existed")
	}

	return &locker
}

func (c *Dlocker) createZnodePath() (string, error) {
	path := c.basePath + "/" + c.prefix
	//save the create unixTime into znode
	nowUnixTime := time.Now().Unix()
	nowUnixTimeBytes := []byte(strconv.FormatInt(nowUnixTime, 10))
	return getZkConn().Create(path, nowUnixTimeBytes, zk.FlagSequence|zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
}

//get the path of minimum serial number znode from sequential children
func (c *Dlocker) getMinZnodePath() (string, error) {
	children, err := c.getPathChildren()
	if err != nil {
		return "", err
	}
	minSNum := modules.GetMinSerialNumber(children, c.prefix)
	minZnodePath := c.basePath + "/" + children[minSNum]
	return minZnodePath, nil
}

//get the children of basePath znode
func (c *Dlocker) getPathChildren() ([]string, error) {
	children, _, err := getZkConn().Children(c.basePath)
	return children, err
}

//get the last znode of created znode
func (c *Dlocker) getLastZnodePath() string {
	return modules.GetLastNodeName(c.lockerPath,
		c.basePath, c.prefix)
}

//just list mutex.Lock()
func (c *Dlocker) Lock() {
	for !c.lock() {
	}
}

//just list mutex.Unlock(), return false when zookeeper connection error or locker timeout
func (c *Dlocker) Unlock() bool {
	return c.unlock()
}

func (c *Dlocker) lock() (isSuccess bool) {
	isSuccess = false
	defer func() {
		e := recover()
		if e == zk.ErrConnectionClosed {
			//try reconnect the zk server
			log.Println("connection closed, reconnect to the zk server")
			reConnectZk()
		}
	}()
	c.innerLock.Lock()
	defer c.innerLock.Unlock()
	//create a znode for the locker path
	var err error
	c.lockerPath, err = c.createZnodePath()
	c.checkErr(err)

	//get the znode which get the lock
	minZnodePath, err := c.getMinZnodePath()
	c.checkErr(err)

	if minZnodePath == c.lockerPath {
		// if the created node is the minimum znode, getLock success
		isSuccess = true
	} else {
		// if the created znode is not the minimum znode,
		// listen for the last znode delete notification
		lastNodeName := c.getLastZnodePath()
		watchPath := c.basePath + "/" + lastNodeName
		isExist, _, watch, err := getZkConn().ExistsW(watchPath)
		c.checkErr(err)
		if isExist {
			select {
			//get lastNode been deleted event
			case event := <-watch:
				if event.Type == zk.EventNodeDeleted {
					//check out the lockerPath existence
					isExist, _, err = getZkConn().Exists(c.lockerPath)
					c.checkErr(err)
					if isExist {
						//checkout the minZnodePath is equal to the lockerPath
						minZnodePath, err := c.getMinZnodePath()
						c.checkErr(err)
						if minZnodePath == c.lockerPath {
							isSuccess = true
						}
					}
				}
			//time out
			case <-time.After(c.timeout):
				// if timeout, delete the timeout znode
				children, err := c.getPathChildren()
				c.checkErr(err)
				for _, child := range children {
					data, _, err := getZkConn().Get(c.basePath + "/" + child)
					if err != nil {
						continue
					}
					if modules.CheckOutTimeOut(data, c.timeout) {
						err := getZkConn().Delete(c.basePath+"/"+child, 0)
						if err == nil {
							log.Println("timeout delete:", c.basePath+"/"+child)
						}
					}
				}
			}
		} else {
			// recheck the min znode
			// the last znode may be deleted too fast to let the next znode cannot listen to it deletion
			minZnodePath, err := c.getMinZnodePath()
			c.checkErr(err)
			if minZnodePath == c.lockerPath {
				isSuccess = true
			}
		}
	}

	return
}

func (c *Dlocker) unlock() (isSuccess bool) {
	isSuccess = false
	defer func() {
		e := recover()
		if e == zk.ErrConnectionClosed {
			//try reconnect the zk server
			log.Println("connection closed, reconnect to the zk server")
			reConnectZk()
		}
	}()
	err := getZkConn().Delete(c.lockerPath, 0)
	if err == zk.ErrNoNode {
		isSuccess = false
		return
	} else {
		c.checkErr(err)
	}
	isSuccess = true
	return
}

func (c *Dlocker) checkErr(err error) {
	if err != nil {
		log.Println(err)
		panic(err)
	}
}

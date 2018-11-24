// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.
//
// gxetcd encapsulate a etcd lease client
package gxetcd

import (
	"context"
	"strconv"
	"sync"
	"time"
)

import (
	"github.com/AlexStocks/goext/runtime"
	log "github.com/AlexStocks/log4go"
	jerrors "github.com/juju/errors"
	// ecv3 "go.etcd.io/etcd/clientv3"
	ecv3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

var (
	// ErrDeadlock is returned by Lock when trying to lock twice without unlocking first
	ErrDeadlock = jerrors.New("etcd: trying to acquire a lock twice")
	// ErrNotLocked is returned by Unlock when trying to release a lock that has not first be acquired.
	ErrNotLocked = jerrors.New("etcd: not locked")
)

//////////////////////////////////////////
// Lease Client
//////////////////////////////////////////

type ElectionSession struct {
	Path     string
	Session  *concurrency.Session
	Election *concurrency.Election
}

// Client represents a lease kept alive for the lifetime of a client.
// Fault-tolerant applications may use sessions to reason about liveness.
type Client struct {
	client    *ecv3.Client
	opts      *clientOptions
	done      chan struct{}
	mutex     sync.Mutex
	id        ecv3.LeaseID
	cancel    context.CancelFunc
	leaderMap map[string]ElectionSession
	lockMap   map[string]*concurrency.Mutex
}

// NewClient gets the leased session for a client.
func NewClient(client *ecv3.Client, options ...ClientOption) (*Client, error) {
	opts := &clientOptions{ttl: defaultClientTTL, ctx: client.Ctx()}
	for _, opt := range options {
		opt(opts)
	}

	c := &Client{
		client:    client,
		opts:      opts,
		id:        opts.leaseID,
		done:      make(chan struct{}),
		leaderMap: make(map[string]ElectionSession),
		lockMap:   make(map[string]*concurrency.Mutex),
	}
	log.Info("new etcd client %+v", c)

	return c, nil
}

func (c *Client) KeepAlive() (<-chan *ecv3.LeaseKeepAliveResponse, error) {
	c.mutex.Lock()
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	id := c.id
	c.mutex.Unlock()

	if id == ecv3.NoLease {
		rsp, err := c.client.Grant(c.opts.ctx, int64(c.opts.ttl.Seconds()))
		if err != nil {
			return nil, jerrors.Trace(err)
		}
		id = rsp.ID
	}
	c.mutex.Lock()
	c.id = id
	c.mutex.Unlock()

	ctx, cancel := context.WithCancel(c.opts.ctx)
	keepAlive, err := c.client.KeepAlive(ctx, id)
	if err != nil || keepAlive == nil {
		c.client.Revoke(ctx, id)
		c.id = ecv3.NoLease
		cancel()
		return nil, jerrors.Annotatef(err, "etcdv3.KeepAlive(id:%#X)", id)
	}

	c.mutex.Lock()
	c.cancel = cancel
	c.mutex.Unlock()

	return keepAlive, nil
}

// Client is the etcd client that is attached to the session.
func (c *Client) EtcdClient() *ecv3.Client {
	return c.client
}

// Lease is the lease ID for keys bound to the session.
func (c *Client) Lease() ecv3.LeaseID { return c.id }

// TTL return the ttl of Client's lease
func (c *Client) TTL() int64 {
	rsp, err := c.client.TimeToLive(context.TODO(), c.id)
	if err != nil {
		return 0
	}

	return rsp.TTL
}

// if @timeout <= 0, Campaign will loop to get the leadership until success.
func (c *Client) Campaign(basePath string, timeout time.Duration) (ElectionSession, error) {
	var (
		grID      int
		flag      bool
		err       error
		leaderKey string
		ctx       context.Context
		cancel    context.CancelFunc
		session   *concurrency.Session
		election  *concurrency.Election
		es        ElectionSession
	)

	if basePath[0] != '/' {
		basePath = "/" + basePath
	}

	grID = gxruntime.GoID()
	leaderKey = basePath + strconv.Itoa(grID)

	ctx = context.Background()
	if 0 < timeout {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	c.mutex.Lock()
	es, flag = c.leaderMap[leaderKey]
	c.mutex.Unlock()
	if !flag {
		session, err = concurrency.NewSession(c.client)
		if err != nil {
			return es, jerrors.Trace(err)
		}
		election = concurrency.NewElection(session, basePath)
		es = ElectionSession{Path: basePath, Session: session, Election: election}
		c.mutex.Lock()
		c.leaderMap[leaderKey] = es
		c.mutex.Unlock()
	}

	err = es.Election.Campaign(ctx, leaderKey)
	if err != nil {
		session.Close()
		return es, jerrors.Trace(err)
	}

	return es, nil
}

func (c *Client) Resign(basePath string, stop bool) error {
	if basePath[0] != '/' {
		basePath = "/" + basePath
	}

	grID := gxruntime.GoID()
	leaderKey := basePath + strconv.Itoa(grID)
	c.mutex.Lock()
	es, flag := c.leaderMap[leaderKey]
	if stop {
		delete(c.leaderMap, leaderKey)
	}
	c.mutex.Unlock()
	if !flag {
		return jerrors.Annotatef(ErrNotLocked, "gr:%d", grID)
	}

	log.Debug("gr:%d, unlock path:%s", grID, basePath)
	err := es.Election.Resign(context.Background())
	if stop {
		es.Session.Close()
	}
	return jerrors.Trace(err)
}

func (c *Client) CheckLeadership(basePath string) bool {
	if basePath[0] != '/' {
		basePath = "/" + basePath
	}

	grID := gxruntime.GoID()
	leaderKey := basePath + strconv.Itoa(grID)
	c.mutex.Lock()
	es, flag := c.leaderMap[leaderKey]
	c.mutex.Unlock()
	if !flag {
		return false
	}

	return string((<-es.Election.Observe(context.Background())).Kvs[0].Value) == leaderKey
}

func (c *Client) Lock(basePath string) error {
	if basePath[0] != '/' {
		basePath = "/" + basePath
	}

	grID := gxruntime.GoID()
	leaderKey := basePath + strconv.Itoa(grID)

	c.mutex.Lock()
	lock, flag := c.lockMap[leaderKey]
	c.mutex.Unlock()
	if !flag {
		// create two separate sessions for lock competition
		session, err := concurrency.NewSession(c.client)
		if err != nil {
			return jerrors.Trace(err)
		}
		defer session.Close()
		lock = concurrency.NewMutex(session, basePath)
	}

	err := lock.Lock(context.Background())
	if err == nil && !flag {
		c.mutex.Lock()
		c.lockMap[leaderKey] = lock
		c.mutex.Unlock()
	}

	return jerrors.Trace(err)
}

func (c *Client) Unlock(basePath string) error {
	if basePath[0] != '/' {
		basePath = "/" + basePath
	}

	grID := gxruntime.GoID()
	leaderKey := basePath + strconv.Itoa(grID)

	c.mutex.Lock()
	lock, flag := c.lockMap[leaderKey]
	delete(c.lockMap, leaderKey)
	c.mutex.Unlock()
	if !flag {
		return jerrors.Trace(ErrNotLocked)
	}

	return jerrors.Trace(lock.Unlock(context.Background()))
}

// Done returns a channel that closes when the lease is orphaned, expires, or
// is otherwise no longer being refreshed.
func (c *Client) Done() <-chan struct{} { return c.done }

// check whether the session has been closed.
func (c *Client) IsClosed() bool {
	select {
	case <-c.done:
		return true

	default:
		return false
	}
}

// Stop ends the refresh for the session lease. This is useful
// in case the state of the client connection is indeterminate (revoke
// would fail) or when transferring lease ownership.
func (c *Client) Stop() {
	select {
	case <-c.done:
		return

	default:
		c.mutex.Lock()
		if c.cancel != nil {
			c.cancel()
			c.cancel = nil
		}
		close(c.done)
		c.mutex.Unlock()
		return
	}
}

// Close orphans the session and revokes the session lease.
func (c *Client) Close() error {
	c.Stop()
	var err error
	c.mutex.Lock()
	id := c.id
	c.id = ecv3.NoLease
	c.mutex.Unlock()
	if id != ecv3.NoLease {
		// if revoke takes longer than the ttl, lease is expired anyway
		ctx, cancel := context.WithTimeout(c.opts.ctx, c.opts.ttl)
		_, err = c.client.Revoke(ctx, id)
		cancel()
	}

	if err != nil {
		err = jerrors.Annotatef(err, "etcdv3.Remove(lieaseID:%+v)", id)
	}

	return err
}

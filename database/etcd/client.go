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
)

import (
	"github.com/AlexStocks/goext/runtime"
	log "github.com/AlexStocks/log4go"
	jerrors "github.com/juju/errors"
	ecv3 "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
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

// Client represents a lease kept alive for the lifetime of a client.
// Fault-tolerant applications may use sessions to reason about liveness.
type Client struct {
	client  *ecv3.Client
	opts    *clientOptions
	done    chan struct{}
	mutex   sync.Mutex
	id      ecv3.LeaseID
	cancel  context.CancelFunc
	lockMap map[string]*concurrency.Mutex
}

// NewClient gets the leased session for a client.
func NewClient(client *ecv3.Client, options ...ClientOption) (*Client, error) {
	opts := &clientOptions{ttl: defaultClientTTL, ctx: client.Ctx()}
	for _, opt := range options {
		opt(opts)
	}

	c := &Client{
		client:  client,
		opts:    opts,
		id:      opts.leaseID,
		done:    make(chan struct{}),
		lockMap: make(map[string]*concurrency.Mutex),
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

func (c *Client) Lock(basePath string) error {
	if basePath[0] != '/' {
		basePath += "/" + basePath
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
		basePath += "/" + basePath
	}

	grID := gxruntime.GoID()
	leaderKey := basePath + strconv.Itoa(grID)

	c.mutex.Lock()
	lock, flag := c.lockMap[leaderKey]
	if flag {
		delete(c.lockMap, leaderKey)
	}
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

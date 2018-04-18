// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.
//
// ref: https://github.com/coreos/etcd/blob/master/clientv3/concurrency/session.go
package gxetcd

import (
	"context"
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
	ecv3 "github.com/coreos/etcd/clientv3"
	jerrors "github.com/juju/errors"
)

const (
	defaultClientTTL = 60e9
)

type clientOptions struct {
	ttl     time.Duration
	leaseID ecv3.LeaseID
	ctx     context.Context
}

//////////////////////////////////////////
// Lease Client Option
//////////////////////////////////////////

// ClientOption configures Client.
type ClientOption func(*clientOptions)

// WithTTL configures the session's TTL in seconds.
// If TTL is <= 0, the default 60 seconds TTL will be used.
func WithTTL(ttl time.Duration) ClientOption {
	return func(so *clientOptions) {
		if ttl > 0 {
			so.ttl = ttl
		}
	}
}

// WithLease specifies the existing leaseID to be used for the session.
// This is useful in process restart scenario, for example, to reclaim
// leadership from an election prior to restart.
func WithLease(leaseID ecv3.LeaseID) ClientOption {
	return func(so *clientOptions) {
		so.leaseID = leaseID
	}
}

// WithContext assigns a context to the session instead of defaulting to
// using the client context. This is useful for canceling NewClient and
// Close operations immediately without having to close the client. If the
// context is canceled before Close() completes, the session's lease will be
// abandoned and left to expire instead of being revoked.
func WithContext(ctx context.Context) ClientOption {
	return func(so *clientOptions) {
		so.ctx = ctx
	}
}

//////////////////////////////////////////
// Lease Client
//////////////////////////////////////////

// Client represents a lease kept alive for the lifetime of a client.
// Fault-tolerant applications may use sessions to reason about liveness.
type Client struct {
	client *ecv3.Client
	opts   *clientOptions
	id     ecv3.LeaseID

	cancel context.CancelFunc
	done   <-chan struct{}
}

// NewClient gets the leased session for a client.
func NewClient(client *ecv3.Client, options ...ClientOption) (*Client, error) {
	opts := &clientOptions{ttl: defaultClientTTL, ctx: client.Ctx()}
	for _, opt := range options {
		opt(opts)
	}

	id := opts.leaseID
	if id == ecv3.NoLease {
		resp, err := client.Grant(opts.ctx, int64(opts.ttl))
		if err != nil {
			return nil, err
		}
		id = ecv3.LeaseID(resp.ID)
	}

	ctx, cancel := context.WithCancel(opts.ctx)
	keepAlive, err := client.KeepAlive(ctx, id)
	if err != nil || keepAlive == nil {
		cancel()
		return nil, err
	}

	done := make(chan struct{})
	s := &Client{client: client, opts: opts, id: id, cancel: cancel, done: done}

	// keep the lease alive until client error or cancelled context
	go func() {
		defer close(done)
		for {
			select {
			// case c.opts.ctx.Done():
			// 	return
			case msg, ok := <-keepAlive:
				// eat messages until keep alive channel closes
				if !ok {
					log.Info("keep alive channel closed")
					// c.revoke()
					return
				} else {
					log.Debug("Recv msg from keepAlive: %s", msg.String())
				}
			}
		}
	}()

	return s, nil
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
		c.cancel()
		<-c.done
		return
	}
}

// Close orphans the session and revokes the session lease.
func (c *Client) Close() error {
	c.Stop()
	// if revoke takes longer than the ttl, lease is expired anyway
	ctx, cancel := context.WithTimeout(c.opts.ctx, c.opts.ttl)
	_, err := c.client.Revoke(ctx, c.id)
	cancel()
	return jerrors.Annotatef(err, "etcdv3.Remove(lieaseID:%+v)", c.id)
}

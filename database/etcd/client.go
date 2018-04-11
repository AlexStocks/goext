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
)

const (
	defaultLeaseClientTTL = 60e9
)

// LeaseClient represents a lease kept alive for the lifetime of a client.
// Fault-tolerant applications may use sessions to reason about liveness.
type LeaseClient struct {
	client *ecv3.Client
	opts   *leaseClientOptions
	id     ecv3.LeaseID

	cancel context.CancelFunc
	done   <-chan struct{}
}

// NewLeaseClient gets the leased session for a client.
func NewLeaseClient(client *ecv3.Client, options ...LeaseClientOption) (*LeaseClient, error) {
	opts := &leaseClientOptions{ttl: defaultLeaseClientTTL, ctx: client.Ctx()}
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
	s := &LeaseClient{client: client, opts: opts, id: id, cancel: cancel, done: done}

	// keep the lease alive until client error or cancelled context
	go func() {
		defer close(done)
		for {
			select {
			// case s.opts.ctx.Done():
			// 	return
			case msg, ok := <-keepAlive:
				// eat messages until keep alive channel closes
				if !ok {
					log.Info("keep alive channel closed")
					// s.revoke()
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
func (s *LeaseClient) Client() *ecv3.Client {
	return s.client
}

// Lease is the lease ID for keys bound to the session.
func (s *LeaseClient) Lease() ecv3.LeaseID { return s.id }

// Done returns a channel that closes when the lease is orphaned, expires, or
// is otherwise no longer being refreshed.
func (s *LeaseClient) Done() <-chan struct{} { return s.done }

// Stop ends the refresh for the session lease. This is useful
// in case the state of the client connection is indeterminate (revoke
// would fail) or when transferring lease ownership.
func (s *LeaseClient) Stop() {
	s.cancel()
	<-s.done
}

// Close orphans the session and revokes the session lease.
func (s *LeaseClient) Close() error {
	s.Stop()
	// if revoke takes longer than the ttl, lease is expired anyway
	ctx, cancel := context.WithTimeout(s.opts.ctx, s.opts.ttl)
	_, err := s.client.Revoke(ctx, s.id)
	cancel()
	return err
}

type leaseClientOptions struct {
	ttl     time.Duration
	leaseID ecv3.LeaseID
	ctx     context.Context
}

// LeaseClientOption configures LeaseClient.
type LeaseClientOption func(*leaseClientOptions)

// WithTTL configures the session's TTL in seconds.
// If TTL is <= 0, the default 60 seconds TTL will be used.
func WithTTL(ttl time.Duration) LeaseClientOption {
	return func(so *leaseClientOptions) {
		if ttl > 0 {
			so.ttl = ttl
		}
	}
}

// WithLease specifies the existing leaseID to be used for the session.
// This is useful in process restart scenario, for example, to reclaim
// leadership from an election prior to restart.
func WithLease(leaseID ecv3.LeaseID) LeaseClientOption {
	return func(so *leaseClientOptions) {
		so.leaseID = leaseID
	}
}

// WithContext assigns a context to the session instead of defaulting to
// using the client context. This is useful for canceling NewLeaseClient and
// Close operations immediately without having to close the client. If the
// context is canceled before Close() completes, the session's lease will be
// abandoned and left to expire instead of being revoked.
func WithContext(ctx context.Context) LeaseClientOption {
	return func(so *leaseClientOptions) {
		so.ctx = ctx
	}
}

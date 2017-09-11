// Copyright 2016 ~ 2017 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// 2017-08-12 11:57
// Package gxredis provides a redis driver by sentinel
// ref: https://github.com/alexstocks/go-sentinel/blob/master/sentinel.go
package gxredis

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

import (
	"github.com/garyburd/redigo/redis"
	xerrors "github.com/pkg/errors"
)

// Sentinel provides a way to add high availability (HA) to Redis Pool using
// preconfigured addresses of Sentinel servers and name of master which Sentinels
// monitor. It works with Redis >= 2.8.12 (mostly because of ROLE command that
// was introduced in that version, it's possible though to support old versions
// using INFO command).
//
// Example of the simplest usage to contact master "mymaster":
//
//  func newSentinelPool() *redis.Pool {
//  	sntnl := &sentinel.Sentinel{
//  		Addrs:      []string{":26379", ":26380", ":26381"},
//  		Dial: func(addr string) (redis.Conn, error) {
//  			timeout := 500 * time.Millisecond
//  			c, err := redis.DialTimeout("tcp", addr, timeout, timeout, timeout)
//  			if err != nil {
//  				return nil, err
//  			}
//  			return c, nil
//  		},
//  	}
//  	return &redis.Pool{
//  		MaxIdle:     3,
//  		MaxActive:   64,
//  		Wait:        true,
//  		IdleTimeout: 240 * time.Second,
//  		Dial: func() (redis.Conn, error) {
//  			masterAddr, err := sntnl.MasterAddr()
//  			if err != nil {
//  				return nil, err
//  			}
//  			c, err := redis.Dial("tcp", masterAddr)
//  			if err != nil {
//  				return nil, err
//  			}
//  			return c, nil
//  		},
//  		TestOnBorrow: func(c redis.Conn, t time.Time) error {
//  			if !sentinel.TestRole(c, "master") {
//  				return errors.New("Role check failed")
//  			} else {
//  				return nil
//  			}
//  		},
//  	}
//  }

const (
	switchMasterChannel = "+switch-master"
	defaultTimeout      = 10 // seconds
	defaultMaxConnNum   = 32
	defaultMaxIdleNum   = 16
)

type Sentinel struct {
	// Addrs is a slice with known Sentinel addresses.
	Addrs []string

	// Dial is a user supplied function to connect to Sentinel on given address. This
	// address will be chosen from Addrs slice.
	// Note that as per the redis-sentinel client guidelines, a timeout is mandatory
	// while connecting to Sentinels, and should not be set to 0.
	Dial func(addr string) (redis.Conn, error)

	// Pool is a user supplied function returning custom connection pool to Sentinel.
	// This can be useful to tune options if you are not satisfied with what default
	// Sentinel pool offers. See defaultPool() method for default pool implementation.
	// In most cases you only need to provide Dial function and let this be nil.
	Pool func(addr string) *redis.Pool

	mu    sync.RWMutex
	pools map[string]*redis.Pool
	addr  string
}

func NewSentinel(addrs []string) *Sentinel {
	return &Sentinel{
		Addrs: addrs,
		Dial: func(addr string) (redis.Conn, error) {
			timeout := defaultTimeout * time.Second
			// read timeout set to 0 to wait sentinel notify
			c, err := redis.DialTimeout("tcp", addr, timeout, 0, timeout)
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}
}

// NoSentinelsAvailable is returned when all sentinels in the list are exhausted
// (or none configured), and contains the last error returned by Dial (which
// may be nil)
type NoSentinelsAvailable struct {
	lastError error
}

func (ns NoSentinelsAvailable) Error() string {
	if ns.lastError != nil {
		return fmt.Sprintf("redigo: no sentinels available; last error: %s", ns.lastError.Error())
	}
	return fmt.Sprintf("redigo: no sentinels available")
}

// putToTop puts Sentinel address to the top of address list - this means
// that all next requests will use Sentinel on this address first.
//
// From Sentinel guidelines:
//
// The first Sentinel replying to the client request should be put at the
// start of the list, so that at the next reconnection, we'll try first
// the Sentinel that was reachable in the previous connection attempt,
// minimizing latency.
//
// Lock must be held by caller.
func (s *Sentinel) putToTop(addr string) {
	addrs := s.Addrs
	if addrs[0] == addr {
		// Already on top.
		return
	}
	newAddrs := []string{addr}
	for _, a := range addrs {
		if a == addr {
			continue
		}
		newAddrs = append(newAddrs, a)
	}
	s.Addrs = newAddrs
}

// putToBottom puts Sentinel address to the bottom of address list.
// We call this method internally when see that some Sentinel failed to answer
// on application request so next time we start with another one.
//
// Lock must be held by caller.
func (s *Sentinel) putToBottom(addr string) {
	addrs := s.Addrs
	if addrs[len(addrs)-1] == addr {
		// Already on bottom.
		return
	}
	newAddrs := []string{}
	for _, a := range addrs {
		if a == addr {
			continue
		}
		newAddrs = append(newAddrs, a)
	}
	newAddrs = append(newAddrs, addr)
	s.Addrs = newAddrs
}

// defaultPool returns a connection pool to one Sentinel. This allows
// us to call concurrent requests to Sentinel using connection Do method.
func (s *Sentinel) defaultPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     defaultMaxIdleNum,
		MaxActive:   defaultMaxConnNum,
		Wait:        true,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return s.Dial(addr)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func (s *Sentinel) GetConn(addr string) redis.Conn {
	pool := s.poolForAddr(addr)
	return pool.Get()
}

func (s *Sentinel) GetConnByRole(addr string, role RedisRole) (redis.Conn, error) {
	pool := s.poolForAddr(addr)
	conn := pool.Get()
	if !CheckRole(conn, role) {
		return nil, errors.New("Role check failed")
	}

	return conn, nil
}

func (s *Sentinel) poolForAddr(addr string) *redis.Pool {
	s.mu.Lock()
	if s.pools == nil {
		s.pools = make(map[string]*redis.Pool)
	}
	pool, ok := s.pools[addr]
	if ok {
		s.mu.Unlock()
		return pool
	}
	s.mu.Unlock()
	newPool := s.newPool(addr)
	s.mu.Lock()
	p, ok := s.pools[addr]
	if ok {
		s.mu.Unlock()
		return p
	}
	s.pools[addr] = newPool
	s.mu.Unlock()
	return newPool
}

func (s *Sentinel) newPool(addr string) *redis.Pool {
	if s.Pool != nil {
		return s.Pool(addr)
	}
	return s.defaultPool(addr)
}

// close connection pool to Sentinel.
// Lock must be hold by caller.
func (s *Sentinel) close() {
	if s.pools != nil {
		for _, pool := range s.pools {
			pool.Close()
		}
	}
	s.pools = nil
}

func (s *Sentinel) doUntilSuccess(f func(redis.Conn) (interface{}, error)) (interface{}, error) {
	s.mu.RLock()
	addrs := s.Addrs
	s.mu.RUnlock()

	var lastErr error

	for _, addr := range addrs {
		conn := s.GetConn(addr)
		reply, err := f(conn)
		conn.Close()
		if err != nil {
			lastErr = err
			s.mu.Lock()
			pool, ok := s.pools[addr]
			if ok {
				pool.Close()
				delete(s.pools, addr)
			}
			s.putToBottom(addr)
			s.mu.Unlock()
			continue
		}
		s.putToTop(addr)
		return reply, nil
	}

	return nil, NoSentinelsAvailable{lastError: lastErr}
}

// MasterAddr returns an address of @name Redis master instance.
func (s *Sentinel) MasterAddr(name string) (string, error) {
	res, err := s.doUntilSuccess(func(c redis.Conn) (interface{}, error) {
		return queryForMaster(c, name)
	})
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// SlaveAddrs returns a slice with known slave addresses of @name master instance.
func (s *Sentinel) SlaveAddrs(name string) ([]string, error) {
	res, err := s.doUntilSuccess(func(c redis.Conn) (interface{}, error) {
		return queryForSlaveAddrs(c, name)
	})
	if err != nil {
		return nil, err
	}
	return res.([]string), nil
}

// Slaves returns a slice with known slaves of master instance.
func (s *Sentinel) Slaves(name string) ([]*Slave, error) {
	res, err := s.doUntilSuccess(func(c redis.Conn) (interface{}, error) {
		return queryForSlaves(c, name)
	})
	if err != nil {
		return nil, err
	}
	return res.([]*Slave), nil
}

// SentinelAddrs returns a slice of known Sentinel addresses Sentinel server aware of.
func (s *Sentinel) SentinelAddrs(name string) ([]string, error) {
	res, err := s.doUntilSuccess(func(c redis.Conn) (interface{}, error) {
		return queryForSentinels(c, name)
	})
	if err != nil {
		return nil, err
	}
	return res.([]string), nil
}

// GetSentinels retruns redis sentinels
func (s *Sentinel) GetSentinels() []string {
	var addrs = make([]string, len(s.Addrs))
	s.mu.RLock()
	copy(addrs, s.Addrs)
	s.mu.RUnlock()

	return addrs
}

// GetInstances returns redis instances
func (s *Sentinel) GetInstances() ([]Instance, error) {
	res, err := s.doUntilSuccess(func(conn redis.Conn) (interface{}, error) {
		res, err := redis.Values(conn.Do("SENTINEL", "masters"))
		if err != nil {
			return nil, err
		}
		var instance Instance
		instances := make([]Instance, 0)
		for _, a := range res {
			sm, err := redis.StringMap(a, err)
			if err != nil {
				return instances, err
			}
			instance.Name = sm["name"]
			instance.Master = &IPAddr{}
			instance.Master.IP = net.ParseIP(sm["ip"]).String()
			port, err := strconv.Atoi(sm["port"])
			if err != nil {
				return instances, err
			}
			instance.Master.Port = uint32(port)
			instance.Slaves, err = queryForSlaves(conn, instance.Name)
			if err != nil {
				return instances, err
			}
			instances = append(instances, instance)
		}
		return instances, nil
	})
	if err != nil {
		return nil, err
	}

	return res.([]Instance), nil
}

func (s *Sentinel) GetInstanceNames() ([]string, error) {
	res, err := s.doUntilSuccess(func(conn redis.Conn) (interface{}, error) { return getSentileRoles(conn) })
	if err != nil {
		return []string{""}, err
	}

	return res.([]string), nil
}

// AddInstance adds a redis intance to sentinel to monitor it
func (s *Sentinel) AddInstance(
	name string, // instance name
	ip string, // master ip
	port int, // master port
	epoch int, // sentinel epoch
	sdownTime int, // sdown time in second
	failoverTimeout int, // failover time in second
	notifyScript string, // notify script, can be empty string if it does not exist
) error {
	for _, addr := range s.Addrs {
		if err := s.addInstance(addr, name, ip, port, epoch, sdownTime, failoverTimeout, notifyScript); err != nil {
			return err
		}
	}

	return nil
}

func (s *Sentinel) addInstance(
	sentinelAddr string,
	name string,
	ip string,
	port int,
	epoch int,
	sdownTime int,
	failoverTimeout int,
	notifyScript string,
) error {
	conn := s.GetConn(sentinelAddr)
	if conn == nil {
		return fmt.Errorf("can not connect to sentinel instance %s", sentinelAddr)
	}
	defer conn.Close()

	if _, err := conn.Do("sentinel", "monitor", name, ip, port, epoch); err != nil {
		// error: ERR Duplicated master name
		return xerrors.Wrapf(err, "sentinelAddr:%s, command:sentinel monitor %s %s %d %d", sentinelAddr, name, ip, port, epoch)
	}

	if _, err := conn.Do("sentinel", "set", name, "down-after-milliseconds", sdownTime*1000); err != nil {
		return xerrors.Wrapf(err, "sentinelAddr:%s, command:sentinel down-after-milliseconds %s %d", sentinelAddr, name, sdownTime*1000)
	}

	if _, err := conn.Do("sentinel", "set", name, "parallel-syncs", 1); err != nil {
		return xerrors.Wrapf(err, "sentinelAddr:%s, command:sentinel parallel-syncs %s 1", sentinelAddr, name)
	}

	if _, err := conn.Do("sentinel", "set", name, "failover-timeout", failoverTimeout*1000); err != nil {
		return xerrors.Wrapf(err, "sentinelAddr:%s, command:sentinel failover-timeout %s %d", sentinelAddr, name, failoverTimeout*1000)
	}

	if notifyScript != "" {
		if _, err := conn.Do("sentinel", "set", name, "client-reconfig-script", notifyScript); err != nil {
			//  ERR Client reconfiguration script seems non existing or non executable
			return xerrors.Wrapf(err, "sentinelAddr:%s, command:sentinel client-reconfig-script %s %s", sentinelAddr, name, notifyScript)
		}
	}

	return nil
}

// RemoveInstance removes a redis intance from sentinel
func (s *Sentinel) RemoveInstance(name string) error {
	for _, addr := range s.Addrs {
		if err := s.removeInstance(addr, name); err != nil {
			return err
		}
	}

	return nil
}

func (s *Sentinel) removeInstance(sentinelAddr string, name string) error {
	conn := s.GetConn(sentinelAddr)
	if conn == nil {
		return fmt.Errorf("can not connect to sentinel instance %s", sentinelAddr)
	}
	defer conn.Close()

	if _, err := redis.String(conn.Do("sentinel", "remove", name)); err != nil {
		// error: ERR No such master with that name
		return xerrors.Wrapf(err, "remove %s from sentinel %s", name, sentinelAddr)
	}

	return nil
}

// Discover allows to update list of known Sentinel addresses. From docs:
//
// A client may update its internal list of Sentinel nodes following this procedure:
// 1) Obtain a list of other Sentinels for this master using the command SENTINEL sentinels <master-name>.
// 2) Add every ip:port pair not already existing in our list at the end of the list.
func (s *Sentinel) Discover(name string, excludeIPArray []string) error {
	addrs, err := s.SentinelAddrs(name)
	if err != nil {
		return err
	}
	s.mu.Lock()
	for _, addr := range addrs {
		ip, _, _ := net.SplitHostPort(addr)
		if stringInSlice(ip, excludeIPArray) {
			continue
		}
		if !stringInSlice(addr, s.Addrs) {
			s.Addrs = append(s.Addrs, addr)
		}
	}
	s.mu.Unlock()
	return nil
}

// Close closes current connection to Sentinel.
func (s *Sentinel) Close() error {
	s.mu.Lock()
	s.close()
	s.mu.Unlock()
	return nil
}

func (s *Sentinel) subscriptMasterSwitch() (redis.PubSubConn, error) {
	s.mu.RLock()
	addrs := s.Addrs
	s.mu.RUnlock()
	var lastErr error

	for _, addr := range addrs {
		conn := s.GetConn(addr)
		sub := redis.PubSubConn{Conn: conn}
		err := sub.Subscribe(switchMasterChannel)
		if err != nil {
			lastErr = err
			s.mu.Lock()
			pool, ok := s.pools[addr]
			if ok {
				pool.Close()
				delete(s.pools, addr)
			}
			s.putToBottom(addr)
			s.mu.Unlock()
			continue
		}
		s.putToTop(addr)
		return sub, nil
	}

	return redis.PubSubConn{nil}, NoSentinelsAvailable{lastError: lastErr}
}

type MasterSwitchInfo struct {
	Name      string
	OldMaster net.TCPAddr
	NewMaster net.TCPAddr
}

type SentinelWatcher struct {
	pubsub redis.PubSubConn
	sync.Mutex
	sync.WaitGroup
	infoChan chan MasterSwitchInfo
	closed   bool
}

func NewMasterSentinel(conn redis.PubSubConn) *SentinelWatcher {
	return &SentinelWatcher{
		pubsub:   conn,
		closed:   false,
		infoChan: make(chan MasterSwitchInfo, 32),
	}
}

func (ms *SentinelWatcher) Close() error {
	// protect pubsub.Unsubscribe to prevent concurrently called
	ms.Lock()
	// prevent repeatedly call
	if ms.closed {
		ms.Unlock()
		return nil
	}
	ms.pubsub.Unsubscribe(switchMasterChannel) // watch goroutine will exit.
	ms.closed = true
	ms.Unlock()
	// wait watch rontine exit
	ms.Wait()
	return ms.pubsub.Close()
}

func (ms *SentinelWatcher) Watch() (<-chan MasterSwitchInfo, error) {
	var (
		err     error
		tcpAddr *net.TCPAddr
		info    MasterSwitchInfo
	)

	ms.Add(1)
	go func() {
		defer ms.Done()
		for {
			switch reply := ms.pubsub.Receive().(type) {
			case redis.Message:
				p := bytes.Split(reply.Data, []byte(" "))
				if len(p) != 5 {
					continue
				}
				info.Name = string(p[0])

				addr := fmt.Sprintf("%s:%s", string(p[1]), string(p[2]))
				tcpAddr, err = net.ResolveTCPAddr("tcp4", addr)
				if err != nil {
					continue
				}
				info.OldMaster = *tcpAddr

				addr = fmt.Sprintf("%s:%s", string(p[3]), string(p[4]))
				tcpAddr, err = net.ResolveTCPAddr("tcp4", addr)
				if err != nil {
					continue
				}
				info.NewMaster = *tcpAddr

				ms.infoChan <- info

			case error:
				// fmt.Printf("watch goroutine got error!\n")
				close(ms.infoChan)
				return

			case redis.Subscription:
				if reply.Channel == switchMasterChannel &&
					reply.Kind == "unsubscribe" && reply.Count == 0 {
					close(ms.infoChan)
					// fmt.Printf("watch goroutine got unsubscribe!\n")
					return
				}
			}
		}
	}()

	return ms.infoChan, nil
}

func (s *Sentinel) MakeSentinelWatcher() (*SentinelWatcher, error) {
	sub, err := s.subscriptMasterSwitch()
	if err != nil {
		return nil, err
	}

	return NewMasterSentinel(sub), nil
}

func queryForMaster(conn redis.Conn, masterName string) (string, error) {
	res, err := redis.Strings(conn.Do("SENTINEL", "GetConn-master-addr-by-name", masterName))
	if err != nil {
		return "", err
	}
	masterAddr := strings.Join(res, ":")
	return masterAddr, nil
}

func queryForSlaveAddrs(conn redis.Conn, masterName string) ([]string, error) {
	slaves, err := queryForSlaves(conn, masterName)
	if err != nil {
		return nil, err
	}
	slaveAddrs := make([]string, 0)
	for _, slave := range slaves {
		slaveAddrs = append(slaveAddrs, slave.Address())
	}
	return slaveAddrs, nil
}

func queryForSlaves(conn redis.Conn, masterName string) ([]*Slave, error) {
	res, err := redis.Values(conn.Do("SENTINEL", "slaves", masterName))
	if err != nil {
		return nil, err
	}
	slaves := make([]*Slave, 0)
	for _, a := range res {
		sm, err := redis.StringMap(a, err)
		if err != nil {
			return slaves, err
		}
		slave := &Slave{
			Flags: sm["flags"],
			Addr:  new(IPAddr),
		}
		slave.Addr.IP = net.ParseIP(sm["ip"]).String()
		port, err := strconv.Atoi(sm["port"])
		if err != nil {
			return slaves, err
		}
		slave.Addr.Port = uint32(port)
		slaves = append(slaves, slave)
	}
	return slaves, nil
}

func queryForSentinels(conn redis.Conn, masterName string) ([]string, error) {
	res, err := redis.Values(conn.Do("SENTINEL", "sentinels", masterName))
	if err != nil {
		return nil, err
	}
	sentinels := make([]string, 0)
	for _, a := range res {
		sm, err := redis.StringMap(a, err)
		if err != nil {
			return sentinels, err
		}
		sentinels = append(sentinels, fmt.Sprintf("%s:%s", sm["ip"], sm["port"]))
	}
	return sentinels, nil
}

func stringInSlice(str string, slice []string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

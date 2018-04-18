package gxetcd

import (
	"fmt"
	"sync"
	"testing"
)

import (
	etcdv3 "github.com/coreos/etcd/clientv3"
	jerrors "github.com/juju/errors"
	"github.com/stretchr/testify/suite"
	"time"
)

type ClientTestSuite struct {
	suite.Suite
	config etcdv3.Config
	client *Client
	wg     sync.WaitGroup
}

func (suite *ClientTestSuite) SetupSuite() {
	suite.config = etcdv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 8e9,
	}
}

func (suite *ClientTestSuite) SetupTest() {
	etcdClient, err := etcdv3.New(suite.config)
	if err != nil {
		panic(jerrors.Errorf("etcdv3.New(config:%+v) = error:%s", suite.config, err))
	}
	suite.client, _ = NewClient(etcdClient, WithTTL(7e9))
}

func (suite *ClientTestSuite) TearDownTest() {
	suite.client.Close()
}

func (suite *ClientTestSuite) TearDownSuite() {
	suite.client.Close()
	suite.wg.Wait()
}

func (suite *ClientTestSuite) TestClient_TTL() {
	suite.T().Logf("ttl:%s", time.Duration(suite.client.TTL()))
}

func (suite *ClientTestSuite) TestClient_Keepalive() {
	// suite.T().Logf("start to test keep alvie")
	fmt.Println("start to test keep alvie")
	// time.Sleep(180e9) // wait etcd dead
	keepAlive, err := suite.client.KeepAlive()
	suite.Equal(nil, err, "etcd.KeepAlive()")
	suite.wg.Add(1)
	go func() {
		defer suite.wg.Done()
		for {
			select {
			case msg, ok := <-keepAlive:
				// eat messages until keep alive channel closes
				if !ok {
					fmt.Println("keep alive channel closed")
					if suite.client.IsClosed() {
						fmt.Println("keep alive goroutine exit now ...")
						return
					}
					keepAlive, err = suite.client.KeepAlive()
					suite.Equal(nil, err, "etcd.KeepAlive()")
				} else {
					fmt.Printf("Recv msg from keepAlive: %s\n", msg.String())
				}
			case <-time.After(2e9):
				fmt.Println("tick tock ...")
			}
		}
	}()

	time.Sleep(10e9)
	// suite.T().Logf("finish testing keep alvie")
	fmt.Println("finish testing keep alvie")
}

func (suite *ClientTestSuite) TestClient_Close() {
	suite.client.Stop()
	err := suite.client.Close()
	suite.Equal(nil, err, "Client.Close() = error:%s", jerrors.ErrorStack(err))
	flag := suite.client.IsClosed()
	suite.Equal(true, flag)
	err = suite.client.Close()
	suite.Equal(nil, err, "Client.Close() = error:%s", jerrors.ErrorStack(err))
	flag = suite.client.IsClosed()
	suite.Equal(true, flag)
	// suite.T().Logf("Client.Close() = error:%s", jerrors.ErrorStack(err))
}

func TestClientTestSuite(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}

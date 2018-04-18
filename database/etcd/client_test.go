package gxetcd

import (
	"testing"
	"time"
)

import (
	etcdv3 "github.com/coreos/etcd/clientv3"
	jerrors "github.com/juju/errors"
	"github.com/stretchr/testify/suite"
)

type ClientTestSuite struct {
	suite.Suite
	client *Client
}

func (suite *ClientTestSuite) SetupSuite() {
	config := etcdv3.Config{
		Endpoints:   []string{"127.0.0.1:2379", "127.0.0.1:12379", "127.0.0.1:22379"},
		DialTimeout: 5e9,
	}
	etcdClient, err := etcdv3.New(config)
	if err != nil {
		panic(jerrors.Errorf("etcdv3.New(config:%+v) = error:%s", config, err))
	}
	suite.client, _ = NewClient(etcdClient, WithTTL(5e9))
}

func (suite *ClientTestSuite) TearDownSuite() {
	suite.client.Close()
}

func (suite *ClientTestSuite) TestClient_TTL() {
	suite.T().Logf("ttl:%s", time.Duration(suite.client.TTL()))
}

func (suite *ClientTestSuite) TestClient_Close() {
	suite.client.Stop()
	err := suite.client.Close()
	suite.Equal(nil, err, "Client.Close() = error:%s", jerrors.ErrorStack(err))
	flag := suite.client.IsClosed()
	suite.Equal(true, flag)
	err = suite.client.Close()
	suite.NotEqual(nil, err, "Client.Close() = error:%s", jerrors.ErrorStack(err))
	flag = suite.client.IsClosed()
	suite.Equal(true, flag)
	suite.T().Logf("Client.Close() = error:%s", jerrors.ErrorStack(err))
}

func TestClientTestSuite(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}

package gxetcd

import (
	"testing"
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
	suite.T().Logf("ttl:%d", suite.client.TTL())
}

func TestClientTestSuite(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}

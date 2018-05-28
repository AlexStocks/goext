package gxzookeeper

import (
	"testing"
)

import (
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/suite"
	"strings"
)

type ClientTestSuite struct {
	suite.Suite
	client *Client
}

func (suite *ClientTestSuite) SetupSuite() {
}

func (suite *ClientTestSuite) SetupTest() {
	conn, _, _ := zk.Connect([]string{"127.0.0.1:2181"}, 3e9)
	suite.client = NewClient(conn)
}

func (suite *ClientTestSuite) TearDownTest() {
	suite.client.ZkConn().Close()
}

func (suite *ClientTestSuite) TearDownSuite() {
}

func (suite *ClientTestSuite) TestClient_RegisterTempSeq() {
	path := "/test"
	err := suite.client.CreateZkPath(path)
	suite.Equal(nil, err, "CreateZkPath")

	path += "/hello"
	data := "world"
	tempPath, err := suite.client.RegisterTempSeq(path, []byte(data))
	suite.Equal(nil, err, "RegisterTempSeq")
	suite.Equal(true, strings.HasPrefix(tempPath, path), "tempPath:%s", tempPath)

	suite.client.DeleteZkPath(path)
}

func TestClientTestSuite(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}

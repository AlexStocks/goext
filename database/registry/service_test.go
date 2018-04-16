package gxregistry

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type ServiceAddrTestSuite struct {
	suite.Suite
	sa ServiceAttr
}

func (suite *ServiceAddrTestSuite) SetupSuite() {
	suite.sa = ServiceAttr{
		Group:    "bjtelecom",
		Name:     "shopping",
		Protocol: "pb",
		Version:  "1.0.1",
		Role:     Provider,
	}
}

func (suite *ServiceAddrTestSuite) TearDownSuite() {
	suite.sa = ServiceAttr{}
}

func (suite *ServiceAddrTestSuite) TestServiceAttr_Marshal() {
	saBytes, err := suite.sa.Marshal()
	suite.T().Logf("sa string:%#v, err:%#v", string(saBytes), err)
	saStr := "group%3Dbjtelecom%26protocol%3Dpb%26role%3DProvider%26service%3Dshopping%26version%3D1.0.1"
	suite.Equalf([]byte(saStr), saBytes, "Marshal(sa:%+v)", suite.sa)
	suite.Equalf(nil, err, "Marshal(sa:%+v)", suite.sa)
}

func (suite *ServiceAddrTestSuite) TestServiceAttr_Unmarshal() {
	var sa ServiceAttr
	saStr := "group%3Dbjtelecom%26protocol%3Dpb%26role%3DProvider%26service%3Dshopping%26version%3D1.0.1"
	err := (&sa).Unmarshal([]byte(saStr))
	suite.T().Logf("suite.sa:%+v, sa:%+v", suite.sa, sa)
	suite.Equalf(sa, suite.sa, "Unmarshal(sa:%+v)", suite.sa)
	suite.Equalf(nil, err, "Unmarshal(sa:%+v)", suite.sa)
}

func TestServiceAddrTestSuite(t *testing.T) {
	suite.Run(t, new(ServiceAddrTestSuite))
}

// Path example: /dubbo/shopping-bjtelecom-pb-1.0.1/node1
func TestService_NodePath(t *testing.T) {
	ast := assert.New(t)

	node := Node{ID: "node1", Address: "127.0.0.1", Port: 12345}
	service := Service{
		ServiceAttr: ServiceAttr{
			Group:    "bjtelecom",
			Name:     "shopping",
			Protocol: "pb",
			Version:  "1.0.1",
		},
		Nodes: []*Node{&node},
	}
	path := service.Path("/dubbo")
	ast.Equalf("/dubbo/shopping-bjtelecom-pb-1.0.1/", path, "service:%+v, path:%s", service, path)

	path = service.NodePath("/dubbo", node)
	ast.Equalf("/dubbo/shopping-bjtelecom-pb-1.0.1/node1", path, "service:%+v, path:%s", service, path)
}

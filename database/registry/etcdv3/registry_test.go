package gxetcd

import (
	"testing"
	"time"
)

import (
	"github.com/AlexStocks/goext/database/registry"
	"github.com/AlexStocks/goext/log"
	"github.com/stretchr/testify/suite"
)

type RegisterTestSuite struct {
	suite.Suite
	reg  gxregistry.Registry
	sa   gxregistry.ServiceAttr
	node gxregistry.Node
}

func (suite *RegisterTestSuite) SetupSuite() {
	suite.reg = NewRegistry(
		gxregistry.WithAddrs([]string{"127.0.0.1:2379", "127.0.0.1:12379", "127.0.0.1:22379"}...),
		gxregistry.WithTimeout(3e9),
		gxregistry.WithRoot("/etcd_test"),
	)

	suite.sa = gxregistry.ServiceAttr{
		Group:    "bjtelecom",
		Name:     "shopping",
		Protocol: "pb",
		Version:  "1.0.1",
		Role:     gxregistry.Provider,
	}

	suite.node = gxregistry.Node{ID: "node0", Address: "127.0.0.1", Port: 12345}
}

func (suite *RegisterTestSuite) TearDownSuite() {
	suite.reg.Close()
}

func (suite *RegisterTestSuite) TestRegistry_Options() {
	opts := suite.reg.Options()
	suite.Equalf("/etcd_test", opts.Root, "reg.Options.Root")
	suite.Equalf([]string{"127.0.0.1:2379", "127.0.0.1:12379", "127.0.0.1:22379"}, opts.Addrs, "reg.Options.Addrs")
	suite.Equalf(time.Duration(3e9), opts.Timeout, "reg.Options.Timeout")
}

func (suite *RegisterTestSuite) TestRegistry_Register() {
	// register suite.node
	service := gxregistry.Service{ServiceAttr: &suite.sa, Nodes: []*gxregistry.Node{&suite.node}}
	err := suite.reg.Register(service)
	suite.Equalf(nil, err, "Register(service:%+v)", service)
	err = suite.reg.Register(service)
	suite.Equalf(gxregistry.ErrorAlreadyRegister, err, "Register(service:%+v)", service)
	_, flag := suite.reg.(*Registry).exist(service)
	suite.Equalf(true, flag, "Registry.exist(service:%s)", gxlog.PrettyString(service))

	// register node1
	node1 := gxregistry.Node{ID: "node1", Address: "127.0.0.1", Port: 12346}
	service = gxregistry.Service{ServiceAttr: &suite.sa, Nodes: []*gxregistry.Node{&node1}}
	err = suite.reg.Register(service)
	suite.Equalf(nil, err, "Register(service:%+v)", service)
	err = suite.reg.Register(service)
	suite.Equalf(gxregistry.ErrorAlreadyRegister, err, "Register(service:%+v)", service)
	_, flag = suite.reg.(*Registry).exist(service)
	suite.Equalf(true, flag, "Registry.exist(service:%s)", gxlog.PrettyString(service))

	service1, err := suite.reg.GetService(suite.sa)
	suite.Equalf(nil, err, "GetService(ServiceAttr:%#v)", suite.sa)
	suite.Equalf(2, len(service1.Nodes), "GetService(ServiceAttr:%+v)", suite.sa)
	// _, flag = suite.reg.(*Registry).exist(*service1)
	// suite.Equalf(true, flag, "Registry.exist(service:%s)", gxlog.PrettyString(service1))

	// // unregister node1
	// service = gxregistry.Service{ServiceAttr: suite.sa, Nodes: []*gxregistry.Node{&node1}}
	// err = suite.reg.Unregister(service)
	// suite.Equalf(nil, err, "Unregister(service:%+v)", service)
	//
	// services, err = suite.reg.GetService(suite.sa)
	// suite.T().Log("services:", gxlog.ColorSprintln(services))
	// suite.Equalf(nil, err, "GetService(ServiceAttr:%#v)", suite.sa)
	// suite.Equalf(1, len(services), "GetService(ServiceAttr:%+v)", suite.sa)
	// suite.Equalf(1, len(services[0].Nodes), "GetService(ServiceAttr:%+v)", suite.sa)
}

func TestRegisterTestSuite(t *testing.T) {
	suite.Run(t, new(RegisterTestSuite))
}

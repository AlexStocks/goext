package gxpool

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type KeeperTestSuite struct {
	suite.Suite
	keeper *Keeper
}

func (suite *KeeperTestSuite) SetupSuite() {
}

func (suite *KeeperTestSuite) SetupTest() {
	suite.keeper = NewKeeper(4)
}

func (suite *KeeperTestSuite) TearDownTest() {
	suite.keeper.Close()
}

func (suite *KeeperTestSuite) TearDownSuite() {
}

func (suite *KeeperTestSuite) TestKeeper() {
	f := func(d time.Duration) DoTask {
		return func(id int, req interface{}) interface{} {
			time.Sleep(d)
			fmt.Printf("worker id:%d, req:%q\n", id, req.(string))
			return nil
		}
	}

	for i := 0; i < 4; i++ {
		task := NewTask(f(time.Duration(1e9*i)), "f"+strconv.Itoa(i))
		err := suite.keeper.PushTask(task, 1e9)
		suite.Equalf(nil, err, "err != nil")
	}
	time.Sleep(5e9)
}

func TestKeeperTestSuite(t *testing.T) {
	suite.Run(t, new(KeeperTestSuite))
}

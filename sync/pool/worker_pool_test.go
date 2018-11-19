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
	pendingNum := suite.keeper.PendingTaskNum()
	suite.T().Logf("pendingNum = %d", pendingNum)
}

func (suite *KeeperTestSuite) TearDownSuite() {
}

func (suite *KeeperTestSuite) TestKeeper() {
	f := func(d time.Duration, req string) Task {
		return func(id int) {
			time.Sleep(d)
			fmt.Printf("worker id:%d, req:%q\n", id, req)
			return
		}
	}

	for i := 0; i < 8; i++ {
		task := f(time.Duration(1e9*i), "f"+strconv.Itoa(i))
		err := suite.keeper.PushTask(task, 1e9)
		suite.Equalf(nil, err, "err != nil")
	}
	time.Sleep(10e9)
	// pendingNum := suite.keeper.PendingTaskNum()
	// suite.Equalf(0, pendingNum, "pendingNum = %d", pendingNum)
}

func (suite *KeeperTestSuite) TestPendingNum() {
	f := func(d time.Duration, req string) Task {
		return func(id int) {
			time.Sleep(d)
			fmt.Printf("worker id:%d, req:%q\n", id, req)
			return
		}
	}

	for i := 0; i < 12; i++ {
		task := f(time.Duration(1e9*i), "f"+strconv.Itoa(i))
		err := suite.keeper.PushTask(task, 1e9)
		suite.Equalf(nil, err, "err != nil")
	}
	time.Sleep(10e9)
	pendingNum := suite.keeper.PendingTaskNum()
	suite.T().Logf("pendingNum = %d", pendingNum)
	suite.Equalf(true, 1 <= pendingNum, "pendingNum = %d", pendingNum)
}

func (suite *KeeperTestSuite) TestPanic() {
	f := func(d time.Duration, req string) Task {
		return func(id int) {
			time.Sleep(d)
			fmt.Printf("worker id:%d, req:%q\n", id, req)
			if id == 1 {
				panic("PONG! PONG! PONG! I AM DIEING!")
			}
			return
		}
	}

	for i := 0; i < 8; i++ {
		task := f(time.Duration(1e9*i), "f"+strconv.Itoa(i))
		err := suite.keeper.PushTask(task, 1e9)
		suite.Equalf(nil, err, "err != nil")
	}
	time.Sleep(10e9)
	// pendingNum := suite.keeper.PendingTaskNum()
	// suite.Equalf(0, pendingNum, "pendingNum = %d", pendingNum)
}

func TestKeeperTestSuite(t *testing.T) {
	suite.Run(t, new(KeeperTestSuite))
}

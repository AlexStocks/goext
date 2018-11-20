package gxring

import (
	"testing"
)

import (
	"github.com/stretchr/testify/suite"
)

type RingTestSuite struct {
	suite.Suite
	ring *Ring
}

func (suite *RingTestSuite) SetupSuite() {
}

func (suite *RingTestSuite) SetupTest() {
	suite.ring = NewRing(11)
}

func (suite *RingTestSuite) TearDownTest() {
	suite.ring.Clear()
}

func (suite *RingTestSuite) TearDownSuite() {
}

func (suite *RingTestSuite) TestCase1() {
	suite.ring.begin = 2
	suite.ring.end = 2
	suite.ring.Write([]byte("howold"))
	suite.Equalf(2, suite.ring.begin, "ring begin %d != 2", suite.ring.begin)
	suite.Equalf(8, suite.ring.end, "ring end %d != 8", suite.ring.end)
	suite.Equalf(6, suite.ring.Size(), "ring size %d != 6", suite.ring.Size())
	suite.Equalf(4, suite.ring.FreeSize(), "free size %d != 4", suite.ring.FreeSize())
	suite.Equalf(false, suite.ring.Empty(), "ring.Empty() %v != false", suite.ring.Empty())
	suite.Equalf(false, suite.ring.Full(), "ring.Full() %v != false", suite.ring.Full())

	buf := make([]byte, 12)
	size := suite.ring.Read(buf)
	suite.Equal(6, size, "size %d != 6", size)
	suite.Equal("howold", string(buf[:size]), "buf %s != \"howold\"", string(buf[:size]))
	suite.Equalf(8, suite.ring.begin, "ring begin %d != 8", suite.ring.begin)
	suite.Equalf(8, suite.ring.end, "ring end %d != 8", suite.ring.end)
	suite.Equalf(0, suite.ring.Size(), "ring size %d != 0", suite.ring.Size())
	suite.Equalf(10, suite.ring.FreeSize(), "free size %d != 10", suite.ring.FreeSize())
	suite.Equalf(true, suite.ring.Empty(), "ring.Empty() %v != true", suite.ring.Empty())
	suite.Equalf(false, suite.ring.Full(), "ring.Full() %v != false", suite.ring.Full())

	suite.ring.begin = 2
	suite.ring.end = 2
	suite.ring.Write([]byte("howold"))
	buf = make([]byte, 3)
	size = suite.ring.Read(buf)
	suite.Equal(3, size, "size %d != 3", size)
	suite.Equal("how", string(buf[:size]), "buf %s != \"how\"", string(buf[:size]))
	suite.Equalf(5, suite.ring.begin, "ring begin %d != 5", suite.ring.begin)
	suite.Equalf(8, suite.ring.end, "ring end %d != 8", suite.ring.end)
	suite.Equalf(3, suite.ring.Size(), "ring size %d != 3", suite.ring.Size())
	suite.Equalf(7, suite.ring.FreeSize(), "free size %d != 7", suite.ring.FreeSize())
	suite.Equalf(false, suite.ring.Empty(), "ring.Empty() %v != false", suite.ring.Empty())
	suite.Equalf(false, suite.ring.Full(), "ring.Full() %v != false", suite.ring.Full())

	suite.ring.begin = 2
	suite.ring.end = 2
	suite.ring.Write([]byte("howold"))
	buf = make([]byte, 6)
	size = suite.ring.Read(buf)
	suite.Equal(6, size, "size %d != 6", size)
	suite.Equal("howold", string(buf[:size]), "buf %s != \"howold\"", string(buf[:size]))
	suite.Equalf(8, suite.ring.begin, "ring begin %d != 8", suite.ring.begin)
	suite.Equalf(8, suite.ring.end, "ring end %d != 8", suite.ring.end)
	suite.Equalf(0, suite.ring.Size(), "ring size %d != 0", suite.ring.Size())
	suite.Equalf(10, suite.ring.FreeSize(), "free size %d != 10", suite.ring.FreeSize())
	suite.Equalf(true, suite.ring.Empty(), "ring.Empty() %v != true", suite.ring.Empty())
	suite.Equalf(false, suite.ring.Full(), "ring.Full() %v != false", suite.ring.Full())
}

func (suite *RingTestSuite) TestCase2() {
	suite.ring.begin = 7
	suite.ring.end = 7
	suite.ring.Write([]byte("howold"))
	suite.Equalf(2, suite.ring.end, "ring end %d != 2", suite.ring.end)
	suite.Equalf(6, suite.ring.Size(), "ring size %d != 6", suite.ring.Size())
	suite.Equalf(4, suite.ring.FreeSize(), "free size %d != 4", suite.ring.FreeSize())
	suite.Equalf(false, suite.ring.Empty(), "ring.Empty() %v != false", suite.ring.Empty())
	suite.Equalf(false, suite.ring.Full(), "ring.Full() %v != false", suite.ring.Full())

	buf := make([]byte, 12)
	size := suite.ring.Read(buf)
	suite.Equal(6, size, "size %d != 6", size)
	suite.Equal("howold", string(buf[:size]), "buf %s != \"howold\"", string(buf[:size]))
	suite.Equalf(2, suite.ring.begin, "ring begin %d != 2", suite.ring.begin)
	suite.Equalf(2, suite.ring.end, "ring end %d != 2", suite.ring.end)
	suite.Equalf(0, suite.ring.Size(), "ring size %d != 0", suite.ring.Size())
	suite.Equalf(10, suite.ring.FreeSize(), "free size %d != 10", suite.ring.FreeSize())
	suite.Equalf(true, suite.ring.Empty(), "ring.Empty() %v != true", suite.ring.Empty())
	suite.Equalf(false, suite.ring.Full(), "ring.Full() %v != false", suite.ring.Full())

	suite.ring.begin = 7
	suite.ring.end = 7
	suite.ring.Write([]byte("howold"))
	buf = make([]byte, 3)
	size = suite.ring.Read(buf)
	suite.Equal(3, size, "size %d != 3", size)
	suite.Equal("how", string(buf[:size]), "buf %s != \"how\"", string(buf[:size]))
	suite.Equalf(10, suite.ring.begin, "ring begin %d != 10", suite.ring.begin)
	suite.Equalf(2, suite.ring.end, "ring end %d != 2", suite.ring.end)
	suite.Equalf(3, suite.ring.Size(), "ring size %d != 3", suite.ring.Size())
	suite.Equalf(7, suite.ring.FreeSize(), "free size %d != 7", suite.ring.FreeSize())
	suite.Equalf(false, suite.ring.Empty(), "ring.Empty() %v != false", suite.ring.Empty())
	suite.Equalf(false, suite.ring.Full(), "ring.Full() %v != false", suite.ring.Full())

	suite.ring.begin = 7
	suite.ring.end = 7
	suite.ring.Write([]byte("howold"))
	buf = make([]byte, 6)
	size = suite.ring.Read(buf)
	suite.Equal(6, size, "size %d != 6", size)
	suite.Equal("howold", string(buf[:size]), "buf %s != \"howold\"", string(buf[:size]))
	suite.Equalf(2, suite.ring.begin, "ring begin %d != 2", suite.ring.begin)
	suite.Equalf(2, suite.ring.end, "ring end %d != 2", suite.ring.end)
	suite.Equalf(0, suite.ring.Size(), "ring size %d != 0", suite.ring.Size())
	suite.Equalf(10, suite.ring.FreeSize(), "free size %d != 10", suite.ring.FreeSize())
	suite.Equalf(true, suite.ring.Empty(), "ring.Empty() %v != true", suite.ring.Empty())
	suite.Equalf(false, suite.ring.Full(), "ring.Full() %v != false", suite.ring.Full())
}

func (suite *RingTestSuite) TestCase3() {
	suite.ring.begin = 4
	suite.ring.end = 4
	suite.Equalf(0, suite.ring.Size(), "ring size %d != 0", suite.ring.Size())
	suite.Equalf(10, suite.ring.FreeSize(), "free size %d != 10", suite.ring.FreeSize())
	suite.Equalf(true, suite.ring.Empty(), "ring.Empty() %v != true", suite.ring.Empty())
	suite.Equalf(false, suite.ring.Full(), "ring.Full() %v != false", suite.ring.Full())

	buf := make([]byte, 12)
	size := suite.ring.Read(buf)
	suite.Equal(0, size, "size %d != 0", size)
	suite.Equal("", string(buf[:size]), "buf %s != \"\"", string(buf[:size]))
	suite.Equalf(4, suite.ring.begin, "ring begin %d != 4", suite.ring.begin)
	suite.Equalf(4, suite.ring.end, "ring end %d != 4", suite.ring.end)
	suite.Equalf(0, suite.ring.Size(), "ring size %d != 0", suite.ring.Size())
	suite.Equalf(10, suite.ring.FreeSize(), "free size %d != 10", suite.ring.FreeSize())
	suite.Equalf(true, suite.ring.Empty(), "ring.Empty() %v != true", suite.ring.Empty())
	suite.Equalf(false, suite.ring.Full(), "ring.Full() %v != false", suite.ring.Full())

	suite.ring.begin = 4
	suite.ring.end = 4
	suite.ring.Write([]byte(""))
	buf = make([]byte, 3)
	size = suite.ring.Read(buf)
	suite.Equal(0, size, "size %d != 0", size)
	suite.Equal("", string(buf[:size]), "buf %s != \"\"", string(buf[:size]))
	suite.Equalf(4, suite.ring.begin, "ring begin %d != 4", suite.ring.begin)
	suite.Equalf(4, suite.ring.end, "ring end %d != 4", suite.ring.end)
	suite.Equalf(0, suite.ring.Size(), "ring size %d != 0", suite.ring.Size())
	suite.Equalf(10, suite.ring.FreeSize(), "free size %d != 10", suite.ring.FreeSize())
	suite.Equalf(true, suite.ring.Empty(), "ring.Empty() %v != true", suite.ring.Empty())
	suite.Equalf(false, suite.ring.Full(), "ring.Full() %v != false", suite.ring.Full())

	suite.ring.begin = 4
	suite.ring.end = 4
	suite.ring.Write([]byte(""))
	buf = make([]byte, 6)
	size = suite.ring.Read(buf)
	suite.Equal(0, size, "size %d != 0", size)
	suite.Equal("", string(buf[:size]), "buf %s != \"\"", string(buf[:size]))
	suite.Equalf(4, suite.ring.begin, "ring begin %d != 4", suite.ring.begin)
	suite.Equalf(4, suite.ring.end, "ring end %d != 4", suite.ring.end)
	suite.Equalf(0, suite.ring.Size(), "ring size %d != 0", suite.ring.Size())
	suite.Equalf(10, suite.ring.FreeSize(), "free size %d != 10", suite.ring.FreeSize())
	suite.Equalf(true, suite.ring.Empty(), "ring.Empty() %v != true", suite.ring.Empty())
	suite.Equalf(false, suite.ring.Full(), "ring.Full() %v != false", suite.ring.Full())
}

func (suite *RingTestSuite) TestCase4() {
	suite.ring.begin = 5
	suite.ring.end = 5
	flag := suite.ring.Write([]byte("helloworldx"))
	suite.Equal(false, flag)
	flag = suite.ring.Write([]byte("helloworld"))
	suite.Equal(true, flag)
	suite.Equalf(10, suite.ring.Size(), "ring size %d != 10", suite.ring.Size())
	suite.Equalf(0, suite.ring.FreeSize(), "free size %d != 0", suite.ring.FreeSize())
	suite.Equalf(false, suite.ring.Empty(), "ring.Empty() %v != false", suite.ring.Empty())
	suite.Equalf(true, suite.ring.Full(), "ring.Full() %v != true", suite.ring.Full())

	buf := make([]byte, 12)
	size := suite.ring.Read(buf)
	suite.Equal(10, size, "size %d != 10", size)
	suite.Equal("helloworld", string(buf[:size]), "buf %s != \"helloworld\"", string(buf[:size]))
	suite.Equalf(4, suite.ring.begin, "ring begin %d != 4", suite.ring.begin)
	suite.Equalf(4, suite.ring.end, "ring end %d != 4", suite.ring.end)
	suite.Equalf(0, suite.ring.Size(), "ring size %d != 0", suite.ring.Size())
	suite.Equalf(10, suite.ring.FreeSize(), "free size %d != 10", suite.ring.FreeSize())
	suite.Equalf(true, suite.ring.Empty(), "ring.Empty() %v != true", suite.ring.Empty())
	suite.Equalf(false, suite.ring.Full(), "ring.Full() %v != false", suite.ring.Full())

	suite.ring.begin = 5
	suite.ring.end = 5
	suite.ring.Write([]byte("helloworld"))
	buf = make([]byte, 3)
	size = suite.ring.Read(buf)
	suite.Equal(3, size, "size %d != 0", size)
	suite.Equal("hel", string(buf[:size]), "buf %s != \"hel\"", string(buf[:size]))
	suite.Equalf(8, suite.ring.begin, "ring begin %d != 8", suite.ring.begin)
	suite.Equalf(4, suite.ring.end, "ring end %d != 4", suite.ring.end)
	suite.Equalf(7, suite.ring.Size(), "ring size %d != 7", suite.ring.Size())
	suite.Equalf(3, suite.ring.FreeSize(), "free size %d != 3", suite.ring.FreeSize())
	suite.Equalf(false, suite.ring.Empty(), "ring.Empty() %v != false", suite.ring.Empty())
	suite.Equalf(false, suite.ring.Full(), "ring.Full() %v != false", suite.ring.Full())

	suite.ring.begin = 5
	suite.ring.end = 5
	suite.ring.Write([]byte("helloworld"))
	buf = make([]byte, 6)
	size = suite.ring.Read(buf)
	suite.Equal(6, size, "size %d != 6", size)
	suite.Equal("hellow", string(buf[:size]), "buf %s != \"hellow\"", string(buf[:size]))
	suite.Equalf(0, suite.ring.begin, "ring begin %d != 0", suite.ring.begin)
	suite.Equalf(4, suite.ring.end, "ring end %d != 4", suite.ring.end)
	suite.Equalf(4, suite.ring.Size(), "ring size %d != 4", suite.ring.Size())
	suite.Equalf(6, suite.ring.FreeSize(), "free size %d != 6", suite.ring.FreeSize())
	suite.Equalf(false, suite.ring.Empty(), "ring.Empty() %v != false", suite.ring.Empty())
	suite.Equalf(false, suite.ring.Full(), "ring.Full() %v != false", suite.ring.Full())
}

func TestRingTestSuite(t *testing.T) {
	suite.Run(t, new(RingTestSuite))
}

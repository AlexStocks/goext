package gxrand

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestCalRedPacket(t *testing.T) {
	packet, err := CalRedPacket(10, 10)
	if assert.Empty(t, err) {
		t.Logf("red packats:%+v\n", packet)
	} else {
		t.Errorf("CalRedPacket(10, 10) = error{%s}", err)
	}
}

func TestIssue2(t *testing.T) {
	arr, err := CalRedPacket(10, 15)
	if err != nil {
		t.Errorf("CalRedPacket(10, 15) = arr:%+v, err:%v", arr, err)
	}
	// wrong output: red_packet_test.go:25: CalRedPacket(10, 15) = arr:[1 1 1 1 1 1 1 1 1 1 1 1 1 1 -4], err:<nil>
	t.Logf("CalRedPacket(10, 15) = arr:%+v, err:%v", arr, err)
}
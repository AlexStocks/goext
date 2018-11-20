package gxnet

import "testing"

func TestIPItoa(t *testing.T) {
	t.Logf("ip string %q", IPItoa(123456789))
	t.Logf("ip string %d", IPAtoi(IPItoa(123456789)))
}

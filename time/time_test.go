package gxtime

import (
	"testing"
	"time"
)

func TestUnix2Time(t *testing.T) {
	now := time.Now()
	nowUnix := Time2Unix(now)
	tm := Unix2Time(nowUnix)
	// time->unix有精度损失，所以只能在秒级进行比较
	if tm.Unix() != now.Unix() {
		t.Fatalf("@now:%#v, tm:%#v", now, tm)
	}
}

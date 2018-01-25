package gxtime

import (
	//"fmt"
	"sync"
	"testing"
	"time"
)

import (
	"github.com/AlexStocks/goext/log"
	"github.com/stretchr/testify/assert"
)

func TestNewTicker(t *testing.T) {
	var (
		num     int
		wg      sync.WaitGroup
		xassert *assert.Assertions
	)

	Init()

	f := func(d time.Duration, num int) {
		var (
			cw    CountWatch
			index int
		)
		defer func() {
			gxlog.CInfo("duration %d loop %d, timer costs:%dms", d, num, cw.Count()/1e6)
			wg.Done()
		}()

		cw.Start()

		for range NewTicker(d).C {
			index++
			//gxlog.CInfo("idx:%d, tick:%s", index, t)
			if index >= num {
				return
			}
		}
	}

	num = 6
	xassert = assert.New(t)
	wg.Add(num)
	go f(SECOND_MS*1.5, 10)
	go f(SECOND_MS*2.51, 10)
	go f(SECOND_MS*1.5, 40)
	go f(SECOND_MS*0.15, 200)
	go f(SECOND_MS*3, 20)
	go f(SECOND_MS*63, 1)
	time.Sleep(0.001e9)
	xassert.Equal(defaultTimerWheel.TimerNumber(), num, "")
	wg.Wait()
}

func TestTick(t *testing.T) {
	var (
		num     int
		wg      sync.WaitGroup
		xassert *assert.Assertions
	)

	Init()

	f := func(d time.Duration, num int) {
		var (
			cw    CountWatch
			index int
		)
		defer func() {
			gxlog.CInfo("duration %d loop %d, timer costs:%dms", d, num, cw.Count()/1e6)
			wg.Done()
		}()

		cw.Start()

		// for t := range Tick(d)
		for range Tick(d) {
			index++
			//gxlog.CInfo("idx:%d, tick:%s", index, t)
			if index >= num {
				return
			}
		}
	}

	num = 6
	xassert = assert.New(t)
	wg.Add(num)
	go f(SECOND_MS*1.5, 10)
	go f(SECOND_MS*2.51, 10)
	go f(SECOND_MS*1.5, 40)
	go f(SECOND_MS*0.15, 200)
	go f(SECOND_MS*3, 20)
	go f(SECOND_MS*63, 1)
	time.Sleep(0.001e9)
	xassert.Equal(defaultTimerWheel.TimerNumber(), num, "")
	wg.Wait()
}

func TestTickFunc(t *testing.T) {
	var (
		num     int
		cw      CountWatch
		xassert *assert.Assertions
	)

	Init()

	f := func() {
		gxlog.CInfo("timer costs:%dms", cw.Count()/1e6)
	}

	num = 3
	xassert = assert.New(t)
	cw.Start()
	TickFunc(SECOND_MS*0.5, f)
	TickFunc(SECOND_MS*1.3, f)
	TickFunc(SECOND_MS*61.5, f)
	time.Sleep(62e9)
	xassert.Equal(defaultTimerWheel.TimerNumber(), num, "")
}

func TestTicker_Reset(t *testing.T) {
	var (
		ticker  *Ticker
		wg      sync.WaitGroup
		cw      CountWatch
		xassert *assert.Assertions
	)

	Init()

	f := func() {
		defer wg.Done()
		gxlog.CInfo("timer costs:%dms", cw.Count()/1e6)
		gxlog.CInfo("in timer func, timer number:%d", defaultTimerWheel.TimerNumber())
	}

	xassert = assert.New(t)
	wg.Add(1)
	cw.Start()
	ticker = TickFunc(SECOND_MS*1.5, f)
	ticker.Reset(SECOND_MS * 3.5)
	time.Sleep(0.001e9)
	xassert.Equal(defaultTimerWheel.TimerNumber(), 1, "")
	wg.Wait()
}

func TestTicker_Stop(t *testing.T) {
	var (
		ticker  *Ticker
		cw      CountWatch
		xassert assert.Assertions
	)

	Init()

	f := func() {
		gxlog.CInfo("timer costs:%dms", cw.Count()/1e6)
	}

	ticker = TickFunc(SECOND_MS*4.5, f)
	// 添加是异步进行的，所以sleep一段时间再去检测timer number
	time.Sleep(0.001e9)
	xassert.Equal(defaultTimerWheel.TimerNumber(), 1, "")
	time.Sleep(SECOND_MS * 5)
	ticker.Stop()
	// 删除是异步进行的，所以sleep一段时间再去检测timer number
	time.Sleep(0.001e9)
	xassert.Equal(defaultTimerWheel.TimerNumber(), 0, "")
}

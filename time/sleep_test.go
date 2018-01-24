package gxtime

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

import (
	"github.com/AlexStocks/goext/log"
)

func TestNewTimerWheel(t *testing.T) {
	var (
		index int
		wheel *TimerWheel
		cw    CountWatch
	)

	wheel = NewTimerWheel()
	defer func() {
		fmt.Println("timer costs:", cw.Count()/1e6, "ms")
		wheel.Stop()
	}()

	cw.Start()
	for {
		select {
		case <-wheel.After(TimeMillisecondDuration(100)):
			index++
			if index >= 10 {
				return
			}
		}
	}
}

func TestAfter(t *testing.T) {
	var (
		wheel *TimerWheel
		wg    sync.WaitGroup
	)
	wheel = NewTimerWheel()

	defer wheel.Stop()

	f := func(d time.Duration, num int) {
		defer wg.Done()

		var (
			cw    CountWatch
			index int
		)
		defer func() {
			gxlog.CInfo("duration %d loop %d, timer costs:%dms", d, num, cw.Count()/1e6)
			gxlog.CInfo("in timer func, timer number:%d", wheel.TimerNumber())
		}()

		cw.Start()
		for {
			select {
			case <-wheel.After(d):
				index++
				if index >= num {
					return
				}
			}
		}
	}

	wg.Add(6)
	go f(1.5e9, 15)
	go f(2.510e9, 10)
	go f(1.5e9, 40)
	go f(0.15e9, 200)
	go f(3e9, 20)
	go f(63e9, 1)
	time.Sleep(0.1e9)
	gxlog.CInfo("after add 6 timers, timer number:%d", wheel.TimerNumber())
	wg.Wait()
}

func TestAfterFunc(t *testing.T) {
	var (
		wg sync.WaitGroup
		cw CountWatch
	)

	Init()

	f := func() {
		defer wg.Done()
		gxlog.CInfo("timer costs:%dms", cw.Count()/1e6)
		gxlog.CInfo("in timer func, timer number:%d", defaultTimerWheel.TimerNumber())
	}

	wg.Add(3)
	cw.Start()
	AfterFunc(0.5e9, f)
	AfterFunc(1.5e9, f)
	AfterFunc(61.5e9, f)
	time.Sleep(0.1e9)
	gxlog.CInfo("after add 3 timer, timer number:%d", defaultTimerWheel.TimerNumber())
	wg.Wait()
}

func TestTimer_Reset(t *testing.T) {
	var (
		timer *Timer
		wg    sync.WaitGroup
		cw    CountWatch
	)

	Init()

	f := func() {
		defer wg.Done()
		gxlog.CInfo("timer costs:%dms", cw.Count()/1e6)
		gxlog.CInfo("in timer func, timer number:%d", defaultTimerWheel.TimerNumber())
	}

	wg.Add(1)
	cw.Start()
	timer = AfterFunc(1.5e9, f)
	timer.Reset(3.5e9)
	time.Sleep(0.2e9)
	gxlog.CInfo("timer number:%d", defaultTimerWheel.TimerNumber())
	wg.Wait()
}

func TestTimer_Stop(t *testing.T) {
	var (
		timer *Timer
		cw    CountWatch
	)

	Init()

	f := func() {
		gxlog.CInfo("timer costs:%dms", cw.Count()/1e6)
	}

	timer = AfterFunc(4.5e9, f)
	// 添加是异步进行的，所以sleep一段时间再去检测timer number
	time.Sleep(1e9)
	gxlog.CInfo("timer number:%d", defaultTimerWheel.TimerNumber())
	timer.Stop()
	// 删除是异步进行的，所以sleep一段时间再去检测timer number
	time.Sleep(1e9)
	gxlog.CInfo("after stop, timer number:%d", defaultTimerWheel.TimerNumber())
	time.Sleep(3e9)
}

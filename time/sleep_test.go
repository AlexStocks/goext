package gxtime

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

import (
	"github.com/AlexStocks/goext/log"
	"github.com/AlexStocks/goext/time"
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
		case <-After(TimeMillisecondDuration(100)):
			// fmt.Println("loop:", index)
			index++
			if index >= 10 {
				return
			}
		}
	}
}

func TestAfter(t *testing.T) {
	var (
		wheel *gxtime.TimerWheel
		wg    sync.WaitGroup
	)
	wheel = gxtime.NewTimerWheel()

	defer wheel.Stop()

	f := func(d time.Duration) {
		defer wg.Done()

		var cw gxtime.CountWatch
		defer func() {
			gxlog.CInfo("duration %d loop over, timer costs:%dms", d, cw.Count()/1e6)
		}()

		var index int
		cw.Start()
		for {
			select {
			case <-wheel.After(d):
				gxlog.CInfo("loop index:%d, interval:%d", index, d)
				index++
				if index >= 5 {
					return
				}
			}
		}
	}

	wg.Add(2)
	// go f(1.5e9)
	// go f(2.510e9)

	go f(1.5e9)
	go f(3e9)
	wg.Wait()
}

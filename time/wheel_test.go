package gxtime

import (
	"fmt"
	"testing"
)

// output:
// timer costs: 100002 ms
// --- PASS: TestNewWheel (100.00s)
func TestNewWheel(t *testing.T) {
	var (
		index int
		wheel *Wheel
		cw    CountWatch
	)
	wheel = NewWheel(TimeMillisecondDuration(100), 20)
	defer func() {
		fmt.Println("timer costs:", cw.Count()/1e6, "ms")
		wheel.Stop()
	}()

	cw.Start()
	for {
		select {
		case <-wheel.After(TimeMillisecondDuration(1000)):
			fmt.Println("loop:", index)
			index++
			if index >= 3 {
				return
			}
		}
	}
}

// output:
// timer costs: 150001 ms
// --- PASS: TestNewWheel2 (150.00s)
func TestNewWheel2(t *testing.T) {
	var (
		index int
		wheel *Wheel
		cw    CountWatch
	)
	wheel = NewWheel(TimeMillisecondDuration(100), 20)
	defer func() {
		fmt.Println("timer costs:", cw.Count()/1e6, "ms") //
		wheel.Stop()
	}()

	cw.Start()
	for {
		select {
		case <-wheel.After(TimeMillisecondDuration(1510)):
			fmt.Println("loop:", index)
			index++
			if index >= 3 {
				return
			}
		}
	}
}

func TestWheel_After(t *testing.T) {
	var (
		index int
		wheel *Wheel
		cw    CountWatch
	)
	wheel = NewWheel(TimeMillisecondDuration(100), 20)
	defer func() {
		fmt.Println("timer costs:", cw.Count()/1e6, "ms") //
		wheel.Stop()
	}()

	cw.Start()
	for {
		select {
		case <-wheel.After(TimeMillisecondDuration(1510)):
			fmt.Println("loop:", index)
			index++
			if index >= 3 {
				return
			}
		}
	}
}

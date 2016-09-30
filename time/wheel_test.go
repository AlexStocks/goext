package gxtime

import (
	"fmt"
	"testing"
)

func TestNewWheel(t *testing.T) {
	var (
		index int
		wheel *Wheel
	)
	wheel = NewWheel(TimeMillisecondDuration(100), 20)
	defer wheel.Stop()

	for {
		select {
		case <-wheel.After(TimeMillisecondDuration(1000)):
			fmt.Println("loop:", index)
			index++
			if index > 22 {
				return
			}
		}
	}
}

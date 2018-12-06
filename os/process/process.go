// Copyright 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// packaeg gxprocess is used to get process info of "/proc"
package gxprocess

// refs: https://github.com/mitchellh/go-ps/blob/master/process.go

// Process is the generic interface that is implemented on every platform
// and provides common operations for processes.
type Process interface {
	// Pid is the process ID for this process.
	Pid() int

	// PPid is the parent process ID for this process.
	PPid() int

	// Executable name running this process. This is not a path to the
	// executable.
	Executable() string
}

// Processes returns all processes.
//
// This of course will be a point-in-time snapshot of when this method was
// called. Some operating systems don't provide snapshot capability of the
// process table, in which case the process table returned might contain
// ephemeral entities that happened to be running when this was called.
func Processes() ([]Process, error) {
	return processes()
}

// FindProcess looks up a single process by pid.
//
// Process will be nil and error will be nil if a matching process is
// not found.
func FindProcess(pid int) (Process, error) {
	return findProcess(pid)
}

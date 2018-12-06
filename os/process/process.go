// Copyright 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// packaeg gxprocess is used to get process info of "/proc"

// +build linux

package gxprocess

// refs: https://github.com/mitchellh/go-ps/blob/master/process_unix.go

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

// LinuxProcess is an Linux-specific Process information.
type LinuxProcess struct {
	pid   int
	ppid  int
	state rune
	pgrp  int
	sid   int

	binary string
}

func NewLinuxProcess(pid int) (*LinuxProcess, error) {
	dir := fmt.Sprintf("/proc/%d", pid)
	_, err := os.Stat(dir)
	if err != nil {
		// if os.IsNotExist(err) {
		// 	return nil, nil
		// }
		return nil, err
	}

	p := &LinuxProcess{pid: pid}
	return p, p.Refresh()
}
func (p *LinuxProcess) Pid() int {
	return p.pid
}

func (p *LinuxProcess) PPid() int {
	return p.ppid
}

func (p *LinuxProcess) Executable() string {
	return p.binary
}

// Refresh reloads all the data associated with this process.
func (p *LinuxProcess) Refresh() error {
	statPath := fmt.Sprintf("/proc/%d/stat", p.pid)
	dataBytes, err := ioutil.ReadFile(statPath)
	if err != nil {
		return err
	}

	// First, parse out the image name
	data := string(dataBytes)
	binStart := strings.IndexRune(data, '(') + 1
	binEnd := strings.IndexRune(data[binStart:], ')')
	p.binary = data[binStart : binStart+binEnd]

	// Move past the image name and start parsing the rest
	data = data[binStart+binEnd+2:]
	_, err = fmt.Sscanf(data,
		"%c %d %d %d",
		&p.state,
		&p.ppid,
		&p.pgrp,
		&p.sid)

	return err
}

func ProcProcesses() ([]LinuxProcess, error) {
	d, err := os.Open("/proc")
	if err != nil {
		return nil, err
	}
	defer d.Close()

	results := make([]LinuxProcess, 0, 50)
	for {
		fis, err := d.Readdir(10)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		for _, fi := range fis {
			// We only care about directories, since all pids are dirs
			if !fi.IsDir() {
				continue
			}

			// We only care if the name starts with a numeric
			name := fi.Name()
			if name[0] < '0' || name[0] > '9' {
				continue
			}

			// From this point forward, any errors we just ignore, because
			// it might simply be that the process doesn't exist anymore.
			pid, err := strconv.ParseInt(name, 10, 0)
			if err != nil {
				continue
			}

			p, err := NewLinuxProcess(int(pid))
			if err != nil {
				continue
			}

			results = append(results, *p)
		}
	}

	return results, nil
}

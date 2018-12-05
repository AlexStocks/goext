// Copyright 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// packaeg gxsignal encapsulates os/signal related functions.

// +build !windows

package gxos

//refs: https://github.com/alexstocks/supervisord/blob/master/signals/signal.go

import (
	"os"
	"syscall"
)

// ToSignal convert a signal name to signal
func ToSignal(signalName string) (os.Signal, error) {
	if signalName == "HUP" {
		return syscall.SIGHUP, nil
	} else if signalName == "INT" {
		return syscall.SIGINT, nil
	} else if signalName == "QUIT" {
		return syscall.SIGQUIT, nil
	} else if signalName == "KILL" {
		return syscall.SIGKILL, nil
	} else if signalName == "USR1" {
		return syscall.SIGUSR1, nil
	} else if signalName == "USR2" {
		return syscall.SIGUSR2, nil
	} else if signalName == "STOP" {
		return syscall.SIGSTOP, nil
	} else if signalName == "TERM" {
		return syscall.SIGTERM, nil
	}

	return syscall.SIGINT, nil
}

/**
 * description     : Kill send signal @sig to the @process <br/><br/>
 *
 * in-@process     : the process which the signal should be sent to
 * in-@sig         : the signal will be sent
 * in-@sigChildren : true if the signal needs to be sent to the children also
 *
 * out-@err        : if successful, err is nil. Otherwise err.String() instead.
 **/
func Kill(process *os.Process, sig os.Signal, sigChildren bool) error {
	localSig := sig.(syscall.Signal)
	pid := process.Pid
	if sigChildren {
		pid = -pid
	}

	return syscall.Kill(pid, localSig)
}

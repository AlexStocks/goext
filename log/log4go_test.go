/* log_test.go - test for log.go */
package gxlog

import (
	"testing"
)

func TestNewLogger(t *testing.T) {
	// var conf = Conf{Name: "test", Dir: "./log/", Level: "DEBUG", Console: false, Daily: true, BackupNum: 2}
	var conf = Conf{Name: "test", Dir: "./log/", Level: "Info", Console: false, Daily: true, BackupNum: 2}

	var (
		err    error
		logger Logger
	)
	if logger, err = NewLogger(conf); err != nil {
		t.Errorf("NewLogger(conf{%#v}) = error{%#v}", conf, err)
	}

	logger.Debug("Debug")
	logger.Info("Info")
	logger.Warn("Warn")
	logger.Error("Error")
	logger.Critic("Critic")

	logger.Close()
}

func TestAsyncLogger(t *testing.T) {
	// var conf = Conf{Name: "test", Dir: "./log/", Level: "DEBUG", Console: false, Daily: true, BackupNum: 2}
	var conf = Conf{Name: "test", Dir: "./log/", Level: "Info", Console: false, Daily: true, BackupNum: 2, BufSize: 4096}

	var (
		err    error
		logger Logger
	)
	if logger, err = NewLogger(conf); err != nil {
		t.Errorf("NewLogger(conf{%#v}) = error{%#v}", conf, err)
	}

	logger.Debug("Debug")
	logger.Info("Info")
	logger.Warn("Warn")
	logger.Error("Error")
	logger.Critic("Critic")

	logger.Close()
}

func TestNewLoggerWithConfFile(t *testing.T) {
	var (
		conf   string
		logger Logger
	)
	conf = "log_test.xml"
	logger = NewLoggerWithConfFile(conf)

	// And now we're ready!
	logger.Finest("This will only go to those of you really cool UDP kids!  If you change enabled=true.")
	logger.Debug("Oh no!  %d + %d = %d!", 2, 2, 2+2)
	logger.Info("About that time, eh chaps?")
	logger.Warn("Warn")
	logger.Error("Error")
	logger.Critic("Critic")

	logger.Close()
}

func TestMultiLoggers(t *testing.T) {
	var conf = Conf{Name: "test1", Dir: "./log/", Level: "Info", Console: false, Daily: true, BackupNum: 2, BufSize: 4096}

	var (
		err     error
		logger1 Logger
		logger2 Logger
	)
	if logger1, err = NewLogger(conf); err != nil {
		t.Errorf("NewLogger(conf{%#v}) = error{%#v}", conf, err)
	}
	conf = Conf{Name: "test2", Dir: "./log/", Level: "Info", Console: false, Daily: true, BackupNum: 2, BufSize: 4096}
	if logger2, err = NewLogger(conf); err != nil {
		t.Errorf("NewLogger(conf{%#v}) = error{%#v}", conf, err)
	}

	logger1.Debug("Debug")
	logger1.Info("Info")
	logger1.Warn("Warn")
	logger1.Error("Error")
	logger1.Critic("Critic")

	logger2.Debug("Debug")
	logger2.Info("Info")
	logger2.Warn("Warn")
	logger2.Error("Error")
	logger2.Critic("Critic")

	logger2.Close()
	logger1.Close()
}

// go test -v -bench BenchmarkSyncLogger -run=^a
// BenchmarkSyncLogger-4   	   50000	     23154 ns/op
// BenchmarkSyncLogger-4   	   50000	     22340 ns/op
// BenchmarkSyncLogger-4   	   50000	     23471 ns/op
// Avg: 22988 ns/op
func BenchmarkSyncLogger(b *testing.B) {
	var conf = Conf{Name: "test", Dir: "./log/", Level: "DEBUG", Console: false, Daily: true, BackupNum: 2}

	var (
		err    error
		logger Logger
	)

	b.StopTimer()
	if logger, err = NewLogger(conf); err != nil {
		b.Errorf("NewLogger(conf{%#v}) = error{%#v}", conf, err)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		logger.Debug("Debug")
		logger.Info("Info")
		logger.Warn("Warn")
		logger.Error("Error")
		logger.Critic("Critic")
	}

	logger.Close()
	b.StopTimer()
}

// go test -v -bench BenchmarkAsyncLogger -run=^a
//
// bufsize: 1024
// BenchmarkAsyncLogger-4   	  100000	     18486 ns/op
// BenchmarkAsyncLogger-4   	  100000	     18997 ns/op
// BenchmarkAsyncLogger-4   	  100000	     18173 ns/op
// Avg: 18552 ns/op
//
// bufsize: 2048
// BenchmarkAsyncLogger-4   	  100000	     16774 ns/op
// BenchmarkAsyncLogger-4   	  100000	     16416 ns/op
// BenchmarkAsyncLogger-4   	  100000	     18012 ns/op
// Avg: 17067 ns/op
//
// bufsize: 4096
// BenchmarkAsyncLogger-4   	  100000	     17060 ns/op
// BenchmarkAsyncLogger-4   	  100000	     15785 ns/op
// BenchmarkAsyncLogger-4   	  100000	     16543 ns/op
// Avg: 16463 ns/op
//
// bufsize: 8192
// BenchmarkAsyncLogger-4   	  100000	     18529 ns/op
// BenchmarkAsyncLogger-4   	  100000	     18035 ns/op
// BenchmarkAsyncLogger-4   	  100000	     18536 ns/op
// Avg:  18366 ns/op
func BenchmarkAsyncLogger(b *testing.B) {
	var conf = Conf{Name: "test", Dir: "./log/", Level: "DEBUG", Console: false, Daily: true, BackupNum: 2, BufSize: 4096}

	var (
		err    error
		logger Logger
	)

	b.StopTimer()
	if logger, err = NewLogger(conf); err != nil {
		b.Errorf("NewLogger(conf{%#v}) = error{%#v}", conf, err)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		logger.Debug("Debug")
		logger.Info("Info")
		logger.Warn("Warn")
		logger.Error("Error")
		logger.Critic("Critic")
	}

	logger.Close()
	b.StopTimer()
}

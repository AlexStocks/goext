/* log_test.go - test for log.go */
package log

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

func TestNewLoggerWithConfFile(t *testing.T) {

	var (
		err    error
		dir    string
		conf   string
		logger Logger
	)
	dir = "./log"
	conf = "log_test.xml"
	if logger, err = NewLoggerWithConfFile(dir, conf); err != nil {
		t.Errorf("NewLoggerWithConfFile(dir{%s}, conf{%#v}) = error{%#v}", dir, conf, err)
	}

	// And now we're ready!
	logger.Finest("This will only go to those of you really cool UDP kids!  If you change enabled=true.")
	logger.Debug("Oh no!  %d + %d = %d!", 2, 2, 2+2)
	logger.Info("About that time, eh chaps?")
	logger.Warn("Warn")
	logger.Error("Error")
	logger.Critic("Critic")

	logger.Close()
}

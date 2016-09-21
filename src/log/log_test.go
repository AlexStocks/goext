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

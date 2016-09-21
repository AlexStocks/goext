/* log_test.go - test for log.go */
package log

import (
	"testing"
)

func TestNewLogger(t *testing.T) {
	var conf = Conf{Name: "test", Dir: "./log/", Level: "DEBUG", Console: false, Daily: true, BackupNum: 2}

	var (
		err    error
		logger Logger
	)
	if logger, err = NewLogger(conf); err != nil {
		t.Errorf("NewLogger(conf{%#v}) = error{%#v}", conf, err)
	}

	for i := 0; i < 10; i++ {
		logger.Warn("warning msg: %d", i)
		logger.Info("info msg: %d", i)
	}

	logger.Close()
}

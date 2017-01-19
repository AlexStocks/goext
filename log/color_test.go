/* log_test.go - test for log.go */
package gxlog

import (
	"testing"
)

func TestColorLog(t *testing.T) {
	CDebug("Debug")
	CInfo("Info")
	CWarn("Warn")
	CError("Error")
}

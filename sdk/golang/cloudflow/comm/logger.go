package comm

import (
	"fmt"
	"log"
	"os"
	"runtime/debug"
)

var flags = log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile
var logger = log.New(os.Stderr, "cloudflow:", flags)
var enable_log = true

func DisableLog() {
	enable_log = false
}

func EnableLog() {
	enable_log = true
}

func Info(args ...interface{}) {
	logger.Output(2, fmt.Sprintln(args...))
}

func Debug(args ...interface{}) {
	if !enable_log {
		return
	}
	logger.Output(2, fmt.Sprintln(args...))
}

func Log(args ...interface{}) {
	if !enable_log {
		return
	}
	logger.Output(2, fmt.Sprintln(args...))
}

func Err(args ...interface{}) {
	logger.Output(2, fmt.Sprintln(args...))
	os.Exit(1)
}

func Errf(f string, args ...interface{}) {
	logger.Output(2, fmt.Sprintf(f, args...))
	debug.PrintStack()
	os.Exit(1)
}

func LogSetNewLogger(lg *log.Logger) {
	logger = lg
}

func LogSetPrefix(prefix string) {
	logger.SetPrefix(prefix)
}

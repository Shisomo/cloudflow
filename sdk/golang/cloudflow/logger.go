package cloudflow

import (
	"fmt"
	"log"
	"os"
	"runtime/debug"
)

var flags = log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile
var logger = log.New(os.Stderr, "cloudflow:", flags)

func Log(args ...interface{}) {
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

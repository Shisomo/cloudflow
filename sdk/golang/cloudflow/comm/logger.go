package comm

import (
	"fmt"
	"os"
	"runtime/debug"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// var flags = log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile
// var logger = log.New(os.Stderr, "cloudflow:", flags)
var enable_log = true

const (
	DebugLevel = zapcore.DebugLevel
	InfoLevel  = zapcore.InfoLevel
	WarnLevel  = zapcore.WarnLevel
	ErrorLevel = zapcore.ErrorLevel
	PanicLevel = zapcore.PanicLevel
	FatalLevel = zapcore.FatalLevel
)

type CFLogger struct {
	zapLogger   *zap.Logger
	atomicLevel *zap.AtomicLevel
}

func NewCFLogger(level zapcore.Level) *CFLogger {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderCfg.EncodeLevel = zapcore.CapitalLevelEncoder
	atomicLevel := zap.NewAtomicLevelAt(level)
	// 文件和stderr双输出
	file, _ := os.OpenFile("./test.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	syncFile := zapcore.AddSync(file)
	syncConsole := zapcore.AddSync(os.Stderr)
	writerSyncer := zapcore.NewMultiWriteSyncer(syncConsole, syncFile)
	return &CFLogger{
		zapLogger: zap.New(
			zapcore.NewCore(
				zapcore.NewConsoleEncoder(encoderCfg), writerSyncer, atomicLevel,
			),
		),
		atomicLevel: &atomicLevel,
	}

}

func (cflogger *CFLogger) Debug(msg string, fields ...zap.Field) {
	cflogger.zapLogger.Debug(msg, fields...)
}

func (cflogger *CFLogger) Info(msg string, fields ...zap.Field) {
	cflogger.zapLogger.Info(msg, fields...)
}

func (cflogger *CFLogger) Warn(msg string, fields ...zap.Field) {
	cflogger.zapLogger.Warn(msg, fields...)
}

func (cflogger *CFLogger) Error(msg string, fields ...zap.Field) {
	cflogger.zapLogger.Error(msg, fields...)
}

func (cflogger *CFLogger) Panic(msg string, fields ...zap.Field) {
	cflogger.zapLogger.Panic(msg, fields...)
}

func (cflogger *CFLogger) Fatal(msg string, fields ...zap.Field) {
	cflogger.zapLogger.Fatal(msg, fields...)
}

func (cflogger *CFLogger) SetPerfix(perfix string) {
	cflogger.zapLogger.Named(perfix)
}

func (cflogger *CFLogger) Sync() error {
	return cflogger.zapLogger.Sync()
}

var cfLogger = NewCFLogger(InfoLevel)

func DisableLog() {
	enable_log = false
}
func EnableLog() {
	enable_log = true
}

// 以下要优化

func Info(args ...interface{}) {
	cfLogger.Info(fmt.Sprintln(args...))
	// logger.Output(2, fmt.Sprintln(args...))
}

func Debug(args ...interface{}) {
	if !enable_log {
		return
	}
	cfLogger.Debug(fmt.Sprintln(args...))
	// logger.Output(2, fmt.Sprintln(args...))
}

func Log(args ...interface{}) {
	if !enable_log {
		return
	}
	cfLogger.Info(fmt.Sprintf("%v", args...))

	// logger.Output(2, fmt.Sprintln(args...))
}

func Err(args ...interface{}) {
	cfLogger.Error(fmt.Sprintln(args...))

	// logger.Output(2, fmt.Sprintln(args...))
	os.Exit(1)
}

func Errf(f string, args ...interface{}) {
	cfLogger.Error(fmt.Sprintf(f, args...))

	// logger.Output(2, fmt.Sprintf(f, args...))
	debug.PrintStack()
	os.Exit(1)
}

// func LogSetNewLogger(lg *log.Logger) {
// 	logger = lg
// }

func LogSetPrefix(prefix string) {
	cfLogger.SetPerfix(prefix)
}

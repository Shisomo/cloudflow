package cloudflow

import (
	"crypto/md5"
	"encoding/hex"
	"time"
	"strconv"
	"os"
	"runtime"
	"fmt"
)


func Version() string {
	return "0.01"
}


func TextIcon() string {
	return ""
}


func AsMd5(str string) string{
	var h = md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}


func If(a bool, b, c interface{}) interface{}{
	if a {
		return b
	}
	return c
}


func Timestamp() int64 {
	return time.Now().UnixNano()
}


func TimestampStr() string {
	return strconv.FormatInt(Timestamp(), 10)
}


func Itos(a int) string {
	return strconv.Itoa(a)
}


func I64tos(a int64) string{
	return strconv.FormatInt(a, 10)
}


func Env(name string) string {
	return os.Getenv(name)
}


func EnvAPPUuid() string{
	return Env("CF_APP_UUID")
}


func EnvAPPHost() string{
	return Env("CF_APP_HOST")
}


func EnvAPPPort() string{
	return Env("CF_APP_PORT")
}


func Assert(val bool, f string, msg... interface{}) {
	if !val {
		_, path, line, _ := runtime.Caller(1)
		msg_txt := fmt.Sprintf(f, msg...)
		Errf("Assert Error at: %s:%d, with message: %s", path, line, msg_txt)
	}
}


func CfgKeyPrefix() string {
	return "cf."
}
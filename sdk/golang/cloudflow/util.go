package cloudflow

import (
	"crypto/md5"
	"encoding/hex"
	"time"
	"strconv"
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


func TimestampStr() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}


func Itos(a int) string {
	return strconv.Itoa(a)
}

func I64tos(a int64) string{
	return strconv.FormatInt(a, 10)
}

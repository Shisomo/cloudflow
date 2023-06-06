package cloudflow

import (
	"crypto/md5"
	"encoding/hex"
	"time"
	"strconv"
	"os"
	"runtime"
	"fmt"
	"reflect"
	"encoding/base64"
	"encoding/json"
	"strings"
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
	return "cfapp."
}


func DumpKV(data * map[string]interface{}, result * map[string]interface{}, prefix string, lkey string){
	for k, v := range *data {
		npref := prefix + "." + k
		switch reflect.TypeOf(v).Kind() {
		case reflect.Map:
			v := v.(map[string]interface{})
			sub_result := map[string]interface{}{}
			DumpKV(&v, &sub_result, npref, lkey)
			(*result)[npref] = sub_result
		case reflect.Slice, reflect.Array:
			v := v.([]interface{})
			klist := []string{}
			for _, itm:= range v {
				itm := itm.(map[string]interface{})
				uuid := itm[lkey].(string)
				klist = append(klist, uuid)
				DumpKV(&itm, result, k + "." + uuid, lkey)
			}
			(*result)[npref] = klist
		default:
			(*result)[npref] = v
		}
	}
}

func AsJson(data interface{}) string {
	js, err := json.MarshalIndent(data, "", " ")
	Assert(err == nil, "Marshal error")
	return string(js)
}

func Base64En(msg string) string{
	return base64.StdEncoding.EncodeToString([]byte(msg))
}

func Base64De(msg string) string{
	decoded, err := base64.StdEncoding.DecodeString(msg)
	Assert(err==nil, "Decode base64 error: %s", err)
	return string(decoded)
}

func UpdateCfg(cfg *map[string]interface{}, n_cfg *map[string]interface{}){
	for k, v := range (*n_cfg) {
		a := (*cfg)[k]
		switch reflect.TypeOf(v).Kind() {
		case reflect.Map:
			v := v.(map[string]interface{})
			a := a.(map[string]interface{})
			UpdateCfg(&a, &v)
		default:
			(*cfg)[k] = v
			break
		}
	}
}

func SetCfg(cfg *map[string]interface{}, key string, value interface{}){
	Assert(key != "", "key empty")
	keys := strings.Split(key, ".")
	keyl := len(keys)
    for _, k := range keys[:keyl-1] {
		v := (*cfg)[k].(map[string]interface{})
		cfg = &v
	}
	(*cfg)[keys[keyl-1]] = value
}


func GetCfg(cfg *map[string]interface{}, key string) interface{} {
	Assert(key != "", "key empty")
	keys := strings.Split(key, ".")
	keyl := len(keys)
    for _, k := range keys[:keyl-1] {
		v := (*cfg)[k].(map[string]interface{})
		cfg = &v
	}
	return (*cfg)[keys[keyl-1]]
}

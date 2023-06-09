package cloudflow

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/denisbrodbeck/machineid"
)

func Version() string {
	return "0.01"
}

func TextIcon() string {
	return ""
}

func AsMd5(str string) string {
	var h = md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}

func If(a bool, b, c interface{}) interface{} {
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

func I64tos(a int64) string {
	return strconv.FormatInt(a, 10)
}

func Env(name string) string {
	return os.Getenv(name)
}

func EnvAPPUuid() string {
	return Env("CF_APP_UUID")
}

func EnvNodeUuid() string {
	return Env("CF_NODE_UUID")
}

func EnvAPPHost() string {
	return Env("CF_APP_HOST")
}

func EnvAPPPort() string {
	return Env("CF_APP_PORT")
}

func Assert(val bool, f string, msg ...interface{}) {
	if !val {
		_, path, line, _ := runtime.Caller(1)
		msg_txt := fmt.Sprintf(f, msg...)
		Errf("Assert Error at: %s:%d, with message: %s", path, line, msg_txt)
	}
}

func CfgKeyPrefix() string {
	return "cfapp."
}

func DumpKV(data *map[string]interface{}, result *map[string]interface{}, prefix string, lkey string) {
	for k, v := range *data {
		npref := prefix + "." + k
		switch reflect.TypeOf(v).Kind() {
		case reflect.Map:
			v := v.(map[string]interface{})
			DumpKV(&v, data, k, lkey)
		case reflect.Slice, reflect.Array:
			v := v.([]interface{})
			klist := []string{}
			for _, itm := range v {
				itm := itm.(map[string]interface{})
				uuid := itm[lkey].(string)
				klist = append(klist, uuid)
				DumpKV(&itm, result, k+"."+uuid, lkey)
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

func FrJson(data string) interface{} {
	var v interface{}
	err := json.Unmarshal([]byte(data), &v)
	Assert(err == nil, "Unmarshal %s fail", data)
	return v
}

func AsKV(data interface{}) map[string]interface{} {
	return FrJson(AsJson(data)).(map[string]interface{})
}

func Dump(data interface{}, prefix string, lkey string) map[string]interface{} {
	ret := map[string]interface{}{}
	kv := AsKV(data)
	DumpKV(&kv, &ret, prefix, lkey)
	return ret
}

func Base64En(msg string) string {
	return base64.StdEncoding.EncodeToString([]byte(msg))
}

func Base64De(msg string) string {
	decoded, err := base64.StdEncoding.DecodeString(msg)
	Assert(err == nil, "Decode base64 error: %s", err)
	return string(decoded)
}

func UpdateCfg(cfg *map[string]interface{}, n_cfg *map[string]interface{}) {
	for k, v := range *n_cfg {
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

func SetCfg(cfg *map[string]interface{}, key string, value interface{}) {
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

func GetCfgC(cfg *map[string]interface{}, key string) map[string]interface{} {
	return GetCfg(cfg, key).(map[string]interface{})
}

func NodeIP() []string {
	addrs, err := net.InterfaceAddrs()
	Assert(err == nil, "get interface address fail:%s", addrs)
	ips := []string{}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ips = append(ips, ipnet.IP.String())
			}
		}
	}
	if len(ips) < 1 {
		ips = append(ips, "localhost")
	}
	return ips
}

func NodeID() string {
	mid, err := machineid.ID()
	Assert(err == nil, "get machineid fail: %s", mid)
	return mid + "-" + NodeIP()[0]
}

func AppID() string {
	return NodeID() + "-" + Itos(os.Getpid())
}

func AddPrefix(input []string, pre string) []string {
	new := []string{}
	for _, v := range input {
		new = append(new, pre+v)
	}
	return new
}

func ProcessPID(name string) int {
	var out bytes.Buffer
	_, err := exec.LookPath("pidof")
	Assert(err == nil, "pidof not installed")
	cmd := exec.Command("pidof", name)
	cmd.Stdout = &out
	cmd.Run()
	val := strings.TrimSpace(out.String())
	if len(val) < 1 {
		return -1
	}
	v, er := strconv.ParseInt(val, 10, 0)
	Assert(er == nil, "parse pid %s fail: %s", val, er)
	return int(v)
}

func RandInt(mod int) int {
	rand.Seed(time.Now().Unix())
	return rand.Int() % mod
}

func StrListHas(list []string, val string) bool {
	for _, v := range list {
		if v == val {
			return true
		}
	}
	return false
}

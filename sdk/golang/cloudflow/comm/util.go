package comm

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
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

type CFG map[string]interface{}
type DYMAP map[interface{}]interface{}

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

func AsMd5Short(str string) string {
	return AsMd5(str)[:6]
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

func TimeFmt(stamp float64, fmt ...string) string {
	t := time.Unix(int64(stamp)/int64(time.Second), int64(stamp)%int64(time.Second))
	if len(fmt) < 1 {
		return t.Format("2023/7/2-20:54:7")
	}
	return t.Format(fmt[0])
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

func EnvAPPIMP() string {
	return Env("CF_APP_IMP")
}

func EnvAPPScope() string {
	return Env("CF_APP_SCOPE")
}

func Assert(val bool, f string, msg ...interface{}) {
	if !val {
		_, path, line, _ := runtime.Caller(1)
		msg_txt := fmt.Sprintf(f, msg...)
		Errf("Assert Error at: %s:%d, with message: %s", path, line, msg_txt)
	}
}

func DumpKV(data *map[string]interface{}, result *map[string]interface{}, prefix string, lkey string, skey string) {
	for k, v := range *data {
		npref := prefix + "." + k
		switch reflect.TypeOf(v).Kind() {
		case reflect.Map:
			v := v.(map[string]interface{})
			DumpKV(&v, data, k, lkey, skey)
		case reflect.Slice, reflect.Array:
			v := v.([]interface{})
			for _, itm := range v {
				itm := itm.(map[string]interface{})
				uuid := itm[lkey].(string)
				(*result)[DotS(npref, k, uuid)] = itm[skey]
				DumpKV(&itm, result, DotS(k, uuid), lkey, skey)
			}
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

func ListFrJson(data string) []interface{} {
	var v []interface{}
	err := json.Unmarshal([]byte(data), &v)
	Assert(err == nil, "Unmarshal %s fail", data)
	return v
}

func MapFrJson(data string) map[string]interface{} {
	var v map[string]interface{}
	err := json.Unmarshal([]byte(data), &v)
	Assert(err == nil, "Unmarshal %s fail", data)
	return v
}

func Json2Map(data string) map[string]interface{} {
	ret := map[string]interface{}{}
	err := json.Unmarshal([]byte(data), &ret)
	Assert(err == nil, "Unmarshal %s fail", data)
	return ret
}

func AsKV(data interface{}) map[string]interface{} {
	return FrJson(AsJson(data)).(map[string]interface{})
}

func Dump(data interface{}, prefix string, lkey string, skey string) map[string]interface{} {
	ret := map[string]interface{}{}
	kv := AsKV(data)
	DumpKV(&kv, &ret, prefix, lkey, skey)
	return ret
}

func Base64En(msg string) string {
	return base64.StdEncoding.EncodeToString([]byte(msg))
}

func Base64De(msg string) string {
	if msg == "" {
		return msg
	}
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

func Astr(v interface{}) string {
	return fmt.Sprint(v)
}

func SetCfg(cfg *CFG, key string, value interface{}) {
	Assert(key != "", "key empty")
	keys := strings.Split(key, ".")
	keyl := len(keys)
	for _, k := range keys[:keyl-1] {
		v := (*cfg)[k].(CFG)
		cfg = &v
	}
	(*cfg)[keys[keyl-1]] = value
}

func GetCfg(cfg *CFG, key string) interface{} {
	Assert(key != "", "key empty")
	keys := strings.Split(key, ".")
	keyl := len(keys)
	for _, k := range keys[:keyl-1] {
		v := (*cfg)[k].(CFG)
		cfg = &v
	}
	return (*cfg)[keys[keyl-1]]
}

func GetCfgC(cfg *CFG, key string) CFG {
	return GetCfg(cfg, key).(CFG)
}

func convertCFG(y *CFG, x *map[string]interface{}) {
	for k, v := range *x {
		switch reflect.ValueOf(v).Type().Kind() {
		case reflect.Map:
			sub := CFG{}
			tag := v.(map[string]interface{})
			convertCFG(&sub, &tag)
			(*y)[k] = sub
		default:
			(*y)[k] = v
		}
	}
}

func ConvertoCFG(d *map[string]interface{}) CFG {
	a := CFG{}
	convertCFG(&a, d)
	return a
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

func DotS(a ...string) string {
	return strings.Join(a, ".")
}

func ByteHuman(size float64) string {
	if size < math.Pow(1024, 1) {
		return fmt.Sprintf("%.2f B", size/math.Pow(1024, 0))
	}
	if size < math.Pow(1024, 2) {
		return fmt.Sprintf("%.2f KB", size/math.Pow(1024, 1))
	}
	if size < math.Pow(1024, 3) {
		return fmt.Sprintf("%.2f MB", size/math.Pow(1024, 2))
	}
	if size < math.Pow(1024, 4) {
		return fmt.Sprintf("%.2f GB", size/math.Pow(1024, 3))
	}
	return fmt.Sprintf("%.2f TB", size/math.Pow(1024, 4))
}

func AsNatsConString(host string, port interface{}) string {
	if strings.Contains(host, "/") && strings.Contains(host, ":") {
		return host
	}
	return "nats://" + host + ":" + Astr(port)
}

func MergeCFG(cfg ...CFG) CFG {
	if len(cfg) < 1 {
		return CFG{}
	}
	ret := CFG{}
	for _, m := range cfg {
		for k, v := range m {
			ret[k] = v
		}
	}
	return ret
}

func MakeEtcdUrl(host string, port interface{}) string {
	if strings.Contains(host, ":") || strings.Contains(host, "/") {
		return host
	}
	return "http://" + host + ":" + Astr(port)
}

func MakeNatsUrl(host string, port interface{}) string {
	if strings.Contains(host, ":") || strings.Contains(host, "/") {
		return host
	}
	return "nats://" + host + ":" + Astr(port)
}

func FuncName(fc interface{}) string {
	ref_fc := reflect.ValueOf(fc)
	return runtime.FuncForPC(ref_fc.Pointer()).Name()
}

func defValue(tgt reflect.Type) interface{} {
	kd := tgt.Kind()
	switch kd {
	case reflect.Map:
		return reflect.MakeMap(tgt).Interface()
	case reflect.Array, reflect.Slice:
		return reflect.MakeSlice(tgt, 0, 0).Interface()
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return reflect.ValueOf(0).Convert(tgt).Interface()
	case reflect.Bool:
		return false
	case reflect.String:
		return string("")
	case reflect.Ptr:
		return nil
	default:
	}
	Assert(false, "dfault type of type: %s not supported", kd)
	return nil
}

func FuncEmptyRet(fc interface{}) []interface{} {
	ret_type := reflect.ValueOf(fc).Type()
	ret_num := ret_type.NumOut()
	ret := make([]interface{}, ret_num)
	for i := range ret {
		ret[i] = defValue(ret_type.Out(i))
	}
	Assert(ret != nil, "func:%s => %d return is nil", FuncName(fc), ret_type.NumOut())
	return ret
}

func JAsType(data interface{}, tgt reflect.Type) interface{} {
	if data == nil {
		return nil
	}
	ref_data := reflect.ValueOf(data)
	kind := tgt.Kind()
	if Astr(tgt) == Astr(ref_data.Type()) {
		return data
	}
	switch kind {
	case reflect.Slice:
		length := ref_data.Len()
		ret := reflect.MakeSlice(tgt, length, length)
		for i := 0; i < length; i++ {
			value := JAsType(ref_data.Index(i).Interface(), ret.Index(0).Type())
			if value != nil {
				ret.Index(i).Set(reflect.ValueOf(value))
			}
		}
		return ret.Interface()
	case reflect.Map:
		ret := reflect.MakeMap(tgt)
		key_type := tgt.Key()
		val_type := reflect.New(tgt.Elem()).Elem().Type()
		for _, k := range ref_data.MapKeys() {
			ret.SetMapIndex(reflect.ValueOf(JAsType(k.Interface(), key_type)),
				reflect.ValueOf(JAsType(ref_data.MapIndex(k).Interface(), val_type)))
		}
		return ret.Interface()
	case reflect.Bool:
		return bool(data.(bool))
	default:
		return ref_data.Convert(tgt).Interface()
	}
}

func FuncCall(fc interface{}, args []interface{}) []interface{} {
	ref_fc := reflect.ValueOf(fc)
	Assert(ref_fc.Kind() == reflect.Func, "need func")
	args_types := ref_fc.Type()
	args_count := args_types.NumIn()
	Assert(args_count == len(args), "args count not match %d(need) != %d(provide)", args_count, len(args))
	args_value := make([]reflect.Value, args_count)
	for i := range args_value {
		v := args[i]
		if v == nil {
			Assert(false, ">>>>> set nil for arg of %s -> %s", args_types.In(i), FuncName(fc))
			args_value[i] = reflect.ValueOf(v)
			continue
		}
		ned_type := args_types.In(i)
		v = JAsType(v, ned_type)
		val := reflect.ValueOf(v)
		val_type := val.Kind()
		Assert(val_type == ned_type.Kind(), "type not match: %s !=> %s for: %s (%s)", val_type, ned_type, FuncName(fc), v)
		args_value[i] = val
	}
	ret := ref_fc.Call(args_value)
	ret_num := len(ret)
	ned_num := args_types.NumOut()
	Assert(ret_num == ned_num, "output number(%d) error != %d", ret_num, ned_num)
	data := make([]interface{}, ret_num)
	for i, v := range ret {
		data[i] = v.Interface() //ValueOfRefl(v)
	}
	return data
}

func FmStr(f string, a ...interface{}) string {
	return fmt.Sprintf(f, a...)
}

func MakeMsg(index int, data []interface{}, cdata string) string {
	cl_data := map[string]interface{}{
		"index":     index,
		"lang_type": "golang",
		"lang_name": runtime.Version(),
		"ctrl_data": cdata,
		"app_data":  data,
	}
	return Base64En(AsJson(cl_data))
}

func KVMakeMsg(data map[string][]interface{}) map[string]string {
	ret := map[string]string{}
	for k, v := range data {
		Assert(v != nil, "node.ret cannot be nil")
		ret[k] = MakeMsg(-1, v, "defaultv")
	}
	return ret
}

func ParsMsg(data string) map[string]interface{} {
	ret := MapFrJson(Base64De(data))
	ret["index"] = int(ret["index"].(float64))
	return ret
}

func ZipAppend(data [][]interface{}, value []interface{}) [][]interface{} {
	// [A[...], B[...]] <= [A, B]
	if len(data) < 1 {
		ret := make([][]interface{}, len(value))
		for i, v := range value {
			ret[i] = []interface{}{v}
		}
		return ret
	}
	for i, v := range value {
		data[i] = append(data[i], v)
	}
	return data
}

func SzOneDim(data [][]interface{}, sz bool) []interface{} {
	// [A[..], B[..], ...] => [A, B, C[...]]
	ret := make([]interface{}, len(data))
	for i, v := range data {
		if sz {
			Assert(len(v) == 1, "size need be 1: %s", v)
			ret[i] = v[0]
		} else {
			ret[i] = v
		}
	}
	return ret
}

func RandStr(length int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

func UpdateObject(objct interface{}, kw ...map[string]interface{}) {
	var node_rf = reflect.ValueOf(objct)
	for _, arg := range kw {
		for key, value := range arg {
			v := node_rf.Elem().FieldByName(key)
			if v.CanSet() {
				v.Set(reflect.ValueOf(value))
			} else {
				Assert(false, "Set %s fail: k=%s v=%s, %s", node_rf.Type(), key, value, kw)
			}
		}
	}
}

func Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func MKeys(data interface{}) []string {
	data_ref := reflect.ValueOf(data)
	Assert(data_ref.Type().Kind() == reflect.Map, "need map")
	ret := []string{}
	for _, k := range data_ref.MapKeys() {
		ret = append(ret, Astr(k))
	}
	return ret
}

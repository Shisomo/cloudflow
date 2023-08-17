package kvops

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"strings"

	"github.com/nats-io/nats.go"
)

type NatsKVOp struct {
	url   string
	nc    *nats.Conn
	js    nats.JetStreamContext
	scope string
	// st       *nats.StreamInfo
	// subs     map[string]*nats.Subscription
	// pulls    map[string]*nats.Subscription
	// csmr     map[string]*nats.ConsumerInfo
}

// 获取key对应的值
// todo: 补充（*匹配符）模糊查询逻辑
func (ops *NatsKVOp) Get(key string) interface{} {
	// TBD 匹配查询
	// prefix := strings.Contains(key, "*")
	// var e error
	// if prefix {
	// 	key = strings.Replace(key, "*", "", -1)
	// 	val, err := ops.getKV().Get(ops.scope+"."+key)
	// } else {
	// 	val, err := ops.getKV().Get(ops.scope+"."+key)
	// }
	// if e != nil {
	// 	cf.Err("read key fail", e)
	// 	return nil
	// }
	// if len(val.Kvs) < 1 {
	// 	return nil
	// }
	// if prefix {
	// 	ret := map[string]interface{}{}
	// 	for _, v := range v.Kvs {
	// 		ret[strings.Replace(string(v.Key),
	// 			ops.scope+".", "", 1)] = cf.FrJson(cf.Base64De(string(v.Value)))
	// 	}
	// 	return ret
	// }
	val, err := ops.getKV().Get(ops.scope + "." + key)
	cf.Assert(err == nil, "get key(%s) error:%s", key, err)

	return cf.FrJson(cf.Base64De(string(val.Value())))

}
func (ops *NatsKVOp) Set(key string, value interface{}) bool {
	cf.Assert(!strings.Contains(key, "|"), "'|' can not in key")

	_, err := ops.getKV().Put(ops.scope+"."+key, []byte(cf.Base64En(cf.AsJson(value))))
	if err != nil {
		// log
		return false
	}
	return true
}
func (ops *NatsKVOp) Del(key string) bool {
	err := ops.getKV().Delete(key)
	if err != nil {
		// log
		return false
	}
	return true
}
func (ops *NatsKVOp) SetKV(Kv map[string]interface{}, ignore_empty bool) bool {
	for k, v := range Kv {
		if ignore_empty {
			switch v.(type) {
			case int, int8, int16, int32, int64:
				if v.(int) == 0 {
					continue
				}
			case string:
				if v.(string) == "" {
					continue
				}
			default:
			}
		}
		if !ops.Set(k, v) {
			return false
		}
	}
	return true
}
func (ops *NatsKVOp) GetKs(Kv []string, ignore_empty bool) map[string]interface{} {
	ret := map[string]interface{}{}
	for _, k := range Kv {
		v := ops.Get(k)
		if ignore_empty {
			if v == nil {
				continue
			}
			switch v.(type) {
			case int, int8, int16, int32, int64:
				if v.(int) == 0 {
					continue
				}
			case string:
				if v.(string) == "" {
					continue
				}
			default:
			}
		}
		ret[k] = v
	}
	return ret
}
func (ops *NatsKVOp) Host() string {
	return ""
}
func (ops *NatsKVOp) Port() int {
	return -1
}
func (ops *NatsKVOp) Imp() string {
	return "nats"
}
func (ops *NatsKVOp) Scope() string {
	return ops.scope
}

func (ops *NatsKVOp) getKV() nats.KeyValue {
	kv, err := ops.js.KeyValue(cf.AsMd5(ops.scope) + "KV")
	if err == nil {
		return kv
	}
	newkey, err := ops.js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:      cf.AsMd5(ops.scope) + "KV",
		Description: "KV for: " + ops.scope,
		Storage:     nats.FileStorage,
		Replicas:    1,
		Placement:   &nats.Placement{Cluster: "default"},
		MaxBytes:    int64(10e8),
	})
	cf.Assert(err == nil, "create KV(%s) error: %s", ops.scope, err)
	return newkey
}

func NewNatsKVOps(cnn_url string, scope string) *NatsKVOp {
	nc, err := nats.Connect(cnn_url)
	cf.Assert(err == nil, "connet Nats error: %s", err)
	js, err := nc.JetStream()
	cf.Assert(err == nil, "connet Nats jet stream error: %s", err)
	return &NatsKVOp{
		url:   cnn_url,
		nc:    nc,
		js:    js,
		scope: scope,
	}
}

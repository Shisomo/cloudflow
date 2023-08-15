package kvops

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"strings"
	"time"
)

// kv operator
type KVOp interface {
	Get(key string) interface{}
	Set(key string, value interface{}) bool
	Del(key string) bool
	SetKV(Kv map[string]interface{}, ignore_empty bool) bool
	GetKs(Kv []string, ignore_empty bool) map[string]interface{}
	Host() string
	Port() int
	Imp() string
	Scope() string
}

// 获取kv operator实例，通过cfg中imp字段确定是etcd还是redis
func GetKVOpImp(cfg map[string]interface{}) KVOp {
	imp := cfg["imp"].(string)
	switch imp {
	case "etcd":
		host := cfg["host"].(string)
		port := cfg["port"]
		scop := cfg["scope"].(string)
		conn := strings.Split(cf.MakeEtcdUrl(host, port), ",")
		return NewEtcDOps(conn, scop)
	case "redis":
		host := cfg["host"].(string)
		port := cfg["port"]
		scop := cfg["scope"].(string)
		conn := cf.MakeRedisUrl(host, port)
		return NewRedisKVOp(conn, scop)
	case "nats":
		host := cfg["host"].(string)
		port := cfg["port"]
		scop := cfg["scope"].(string)
		conn := cf.MakeNatsUrl(host, port)
		return NewNatsKVOps(conn, scop)
	default:
		cf.Assert(false, "KV %s not support", imp)
	}
	return nil
}

func Lock(ops KVOp, key string, owner string) {
	key_owner := key + ".lock.owner"
	block_time := cf.Timestamp()
	block_flage := 0
	for {
		v := ops.Get(key_owner)
		if v == owner {
			break
		}
		if v == nil || v == "" {
			ops.Set(key_owner, owner)
		}
		wait_time := cf.RandInt(100)
		time.Sleep(time.Microsecond * time.Duration(wait_time))
		delta := (cf.Timestamp() - int64(block_time)) / int64(time.Second)
		if delta > 0 && delta%10 == int64(block_flage) {
			block_flage = int(delta+5) % 10
			cf.Log("waiting lock:", key, delta, "seconds")
		}
	}
	ops.Set(key+".lock.atime", cf.Timestamp())
}

func UnLock(ops KVOp, key string, owner string) {
	key_owner := key + ".lock.owner"
	v := ops.Get(key_owner)
	cf.Assert(v == owner, "Unlocal fail: %s != %s", owner, v)
	ops.Set(key_owner, "")
}

func Touch(ops KVOp, key string) {
	ops.Set(key+".atime", cf.Timestamp())
}

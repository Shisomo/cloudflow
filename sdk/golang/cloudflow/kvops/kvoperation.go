package kvops

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"time"
)

type KVOp interface {
	Get(key string) interface{}
	Set(key string, value interface{}) bool
	Del(key string) bool
	SetKV(Kv map[string]interface{}, ignore_empty bool) bool
	GetKs(Kv []string, ignore_empty bool) map[string]interface{}
}

func GetKVOpImp(imp string, cfg map[string]interface{}) KVOp {
	return nil
}

func Lock(ops KVOp, key string, owner string) {
	key_owner := key + ".lock.owner"
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

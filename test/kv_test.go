package test

import (
	"cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"fmt"
	"testing"
)

func TestKV(t *testing.T) {
	// // redis
	// cfg := comm.CFG{
	// 	"app_id": "app_id",
	// 	"host":   "127.0.0.1",
	// 	"port":   6379,
	// 	"imp":    "redis",
	// 	"scope":  "test",
	// }
	// nats
	cfg := comm.CFG{
		"app_id": "app_id",
		"host":   "127.0.0.1",
		"port":   4222,
		"imp":    "nats",
		"scope":  "test",
	}
	kv := kvops.GetKVOpImp(cfg)
	kv.Set("test1", "test1value")
	kv.Set("test2", "test2value")
	kv.Set("test3", "test3value")
	kv.SetKV(map[string]interface{}{"test4": "test4value", "test5": "test5value", "test6": "test6value"}, true)
	fmt.Printf("test1 的值为：%s\n", kv.Get("test1").(string))
	fmt.Printf("test1&test4 的值为：%s\n", kv.GetKs([]string{"test1", "test4"}, true))
	fmt.Printf("test* 的值为：%v\n", kv.Get("test*"))
}

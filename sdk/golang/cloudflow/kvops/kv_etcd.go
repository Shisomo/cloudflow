package kvops

import (
	"context"
	"time"

	cf "cloudflow/sdk/golang/cloudflow"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcDOps struct {
	scope string
	cli   *clientv3.Client
}

func NewEtcDOps(connUrls []string, scope string) *EtcDOps {
	ops := EtcDOps{
		scope: scope,
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   connUrls,
		DialTimeout: 5 * time.Second,
	})
	cf.Assert(err == nil, "create etcd client fail: %s", err)
	ops.cli = cli
	check_key := "atime" + cf.AsMd5(cf.AppID()) + cf.TimestampStr()
	ops.Set(check_key, check_key)
	rkey := ops.Get(check_key)
	cf.Assert(rkey != nil, "verify etcd fail: %s != %s", check_key, rkey)
	cf.Assert(rkey == check_key, "verify etcd fail: %s != %s", check_key, rkey)
	ops.Del(check_key)
	return &ops
}

func (ops *EtcDOps) contex() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func (ops *EtcDOps) Get(key string) interface{} {
	ctx, cancel := ops.contex()
	v, e := ops.cli.Get(ctx, ops.scope+"."+key)
	defer cancel()
	if e != nil {
		cf.Err("read key fail", e)
		return nil
	}
	if len(v.Kvs) < 1 {
		return nil
	}
	return cf.FrJson(cf.Base64De(string(v.Kvs[0].Value)))
}

func (ops *EtcDOps) Set(key string, value interface{}) bool {
	ctx, cancel := ops.contex()
	_, e := ops.cli.Put(ctx, ops.scope+"."+key, cf.Base64En(cf.AsJson(value)))
	defer cancel()
	if e != nil {
		cf.Err("set key fail:", e)
		return false
	}
	return true
}

func (ops *EtcDOps) Del(key string) bool {
	ctx, cancel := ops.contex()
	_, e := ops.cli.Delete(ctx, ops.scope+"."+key, clientv3.WithPrefix())
	defer cancel()
	if e != nil {
		cf.Err("delete key fail", e)
		return false
	}
	return true
}

func (ops *EtcDOps) SetKV(Kv map[string]interface{}, ignore_empty bool) bool {
	ctx, cancel := ops.contex()
	defer cancel()
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
		_, e := ops.cli.Put(ctx, ops.scope+"."+k, cf.Base64En(cf.AsJson(v)))
		if e != nil {
			cf.Err("set key:", k, "fail:", e)
			return false
		}
	}
	return true
}

func (ops *EtcDOps) GetKs(Kv []string, ignore_empty bool) map[string]interface{} {
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

package kvops

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"context"
	"time"

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
	check_key := "atime" + cf.TimestampStr() + cf.AsMd5(cf.NodeName())
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
	cancel()
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
	cancel()
	if e != nil {
		cf.Err("set key fail:", e)
		return false
	}
	return true
}

func (ops *EtcDOps) Del(key string) bool {
	ctx, cancel := ops.contex()
	_, e := ops.cli.Delete(ctx, ops.scope+"."+key, clientv3.WithPrefix())
	cancel()
	if e != nil {
		cf.Err("delete key fail", e)
		return false
	}
	return true
}

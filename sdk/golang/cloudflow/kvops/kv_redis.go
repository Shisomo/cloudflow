package kvops

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// redis kv实例
type RedisKVOp struct {
	rc         redis.Client
	scope      string
	ctx        context.Context //上下文
	expiration time.Duration   // 过期时间
}

func NewRedisKVOp(connUrls string, scope string) *RedisKVOp {
	opt, err := redis.ParseURL(connUrls)
	cf.Assert(err == nil, "")
	rdb := redis.NewClient(opt)
	ctx := context.Background()
	exp := time.Hour * 10
	return &RedisKVOp{
		rc:         *rdb,
		scope:      scope,
		ctx:        ctx,
		expiration: exp,
	}
}
func (ops *RedisKVOp) Get(key string) interface{} {
	ret, err := ops.rc.Get(ops.ctx, key).Result()
	cf.Assert(err == nil, "get key(%s) error:%s", key, err)
	return ret
}
func (ops *RedisKVOp) Set(key string, value interface{}) bool {
	err := ops.rc.Set(ops.ctx, key, value, ops.expiration).Err()
	if err != nil {
		// log
		return false
	}
	return true
}
func (ops *RedisKVOp) Del(key string) bool {
	err := ops.rc.Del(ops.ctx, key).Err()
	if err != nil {
		// log
		return false
	}
	return true
}
func (ops *RedisKVOp) SetKV(Kv map[string]interface{}, ignore_empty bool) bool {
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
		err := ops.rc.Set(ops.ctx, k, []byte(cf.AsJson(v)), ops.expiration).Err()
		if err != nil {
			cf.Err("set key:", k, "fail:", err)
			return false
		}
	}
	return true
}
func (ops *RedisKVOp) GetKs(Kv []string, ignore_empty bool) map[string]interface{} {
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
func (ops *RedisKVOp) Host() string {
	return ""
}
func (ops *RedisKVOp) Port() int {
	return -1
}
func (ops *RedisKVOp) Imp() string {
	return "redis"
}
func (ops *RedisKVOp) Scope() string {
	return ops.scope
}

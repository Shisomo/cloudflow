package kvops

import "github.com/redis/go-redis/v9"

type RedisKVOp struct {
	redis redis.Client
}

package service

import (
	cf "cloudflow/sdk/golang/cloudflow"
)

type StateOps interface {
	ServiceOps
	ClearAll()
	Get(key string) interface{}
	Set(key string) bool
}


func GetStateSvr(cfg map[string]interface{}) ServiceOps{
	imp := cfg["imp"].(string)
	cf.Log("create state service with imp:", imp)
	switch  imp {
	case "etcd":
		return NewStateEtcd(cfg)
	default:
		cf.Assert(false, "imp:%s not find", imp)
	}
	return nil
}

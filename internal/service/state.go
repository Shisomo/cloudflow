package service

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	kv "cloudflow/sdk/golang/cloudflow/kvops"
)

type StateOps interface {
	ServiceOps
	kv.KVOp
}

func GetStateImp(cfg map[string]interface{}) ServiceOps {
	imp := cfg["imp"].(string)
	cf.Log("create state service with imp:", imp)
	switch imp {
	case "etcd":
		return NewStateEtcd(cfg)
	default:
		cf.Assert(false, "imp:%s not find", imp)
	}
	return nil
}

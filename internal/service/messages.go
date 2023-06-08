package service

import (
	cf "cloudflow/sdk/golang/cloudflow"
)

type Message interface {
	ServiceOps
}

func GetMessageImp(cfg map[string]interface{}) Message {
	imp := cfg["imp"].(string)
	cf.Log("create Message service with imp:", imp)
	switch imp {
	case "nats":
		return NewMessageNats(cfg)
	default:
		cf.Assert(false, "imp:%s not find", imp)
	}
	return nil
}

package service

import (
	"cloudflow/sdk/golang/cloudflow/chops"
	cf "cloudflow/sdk/golang/cloudflow/comm"
)

type MessageService interface {
	ServiceOps
	GetChannelOps() chops.ChannelOp
}

func GetMessageImp(cfg map[string]interface{}) MessageService {
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

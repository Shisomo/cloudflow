package chops

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"time"
)

type ChannelOp interface {
	Get(who string, ch_name []string, timeout time.Duration) []string
	Put(ch_name []string, value string) bool
	Watch(who string, ch_name []string, fc func(worker string, subj string, data string) bool) []string
	Sub(who string, ch_name []string, fc func(worker string, subj string, data string) bool) []string
	Close() bool
	CStop(cnkey []string) bool
	CEmpty(cnkey []string) bool
}

func GetChOpsImp(cfg cf.CFG) ChannelOp {
	imp := cfg["imp"].(string)
	switch imp {
	case "nats":
		host := cfg["host"].(string)
		port := cfg["port"]
		//初始化避免后续报错
		stream_name, ok := cfg["app_id"].(string)
		if !ok {
			stream_name = "app_id"
		}
		return NewNatsChOp(cf.MakeNatsUrl(host, port), stream_name)
	default:
		cf.Assert(false, "%s ChOPs not supported", imp)
	}
	return nil
}

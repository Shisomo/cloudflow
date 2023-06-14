package chops

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
)

type ChannelOp interface {
	Put(ch_name []string, value string) bool
	Watch(ch_name []string, fc func(worker string, subj string, data string) bool) []string
	Close() bool
	CStop(cnkey []string) bool
}

func GetChOpsImp(imp string, cfg cf.CFG) ChannelOp {
	switch imp {
	case "nats":
		host := cfg["host"].(string)
		port := cfg["port"]
		stream_name := cfg["app_id"].(string)
		return NewNatsChOp(cf.MakeNatsUrl(host, port), stream_name)
	default:
		cf.Assert(false, "%s ChOPs not supported", imp)
	}
	return nil
}

package schedule

import (
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/kvops"
)

func StartScheduler(cfg cf.CFG, ops kvops.KVOp) {
	imp := cfg["imp"]
	switch imp {
	case "dumy_scheduler":
		NewDumySche(ops).Run()
	default:
		cf.Assert(false, "scheduler: %s not support", imp)
	}
}

func TryStartSchduler(cfg cf.CFG, ops kvops.KVOp) {
	sche := cfmodule.ListKeys(ops, cf.K_CF_SCHEDUS, cf.K_STAT_WORK)
	if len(sche) < 1 {
		cf.Log("no schadulers find, create new one")
		StartScheduler(cfg, ops)
	}
	cf.Log("runing schedulers:", cfmodule.ListKeys(ops, cf.K_CF_SCHEDUS, cf.K_STAT_WORK))
}

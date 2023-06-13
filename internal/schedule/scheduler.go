package schedule

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	"cloudflow/sdk/golang/cloudflow/kvops"
)

func StartScheduler(cfg map[string]interface{}, ops kvops.KVOp) {
	imp := cfg["imp"]
	switch imp {
	case "dumy_scheduler":
		NewDumySche(ops).Run()
	default:
		cf.Assert(false, "scheduler: %s not support", imp)
	}
}

func TryStartSchduler(cfg map[string]interface{}, ops kvops.KVOp) {
	sche := cfmodule.ListKeys(ops, cf.K_CF_SCHEDUS, cf.K_STAT_WORK)
	if len(sche) < 1 {
		cf.Log("no schadulers find, create new one")
		StartScheduler(cfg, ops)
	}
	cf.Log("runing schedulers:", cfmodule.ListKeys(ops, cf.K_CF_SCHEDUS, cf.K_STAT_WORK))
}

package schedule

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"cloudflow/sdk/golang/cloudflow/schedule"
)

func StartScheduler(cfg map[string]interface{}, ops kvops.KVOp) {
	imp := cfg["imp"]
	switch imp {
	case "dumy":
		NewDumySche(ops).Run()
	default:
		cf.Assert(false, "scheduler: %s not support", imp)
	}
}

func TryStartSchduler(cfg map[string]interface{}, ops kvops.KVOp) {
	sche := schedule.ListScheduler(ops)
	if len(sche) < 1 {
		cf.Log("no schadulers find, create new one")
		StartScheduler(cfg, ops)
	}
	cf.Log("runing schedulers:", schedule.ListScheduler(ops))
}

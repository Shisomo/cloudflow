package worker

import (
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/kvops"
)

func StartWorker(cfg cf.CFG, fcfg cf.CFG, ops kvops.KVOp) {
	imp := cfg["imp"]
	switch imp {
	case "dumy_worker":
		NewDumyWorker(ops, fcfg).Run()
	default:
		cf.Assert(false, "woker: %s not support", imp)
	}
}

func TryStartWorker(worker_cfg cf.CFG, fileops_cfg cf.CFG, ops kvops.KVOp) {
	sche := cfmodule.ListKeys(ops, cf.K_CF_WORKERS, cf.K_STAT_WORK)
	if len(sche) < 1 {
		cf.Log("no workers find, create new one")
		StartWorker(worker_cfg, fileops_cfg, ops)
	}
	cf.Log("runing workers:", cfmodule.ListKeys(ops, cf.K_CF_WORKERS, cf.K_STAT_WORK))
}

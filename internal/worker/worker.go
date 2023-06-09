package worker

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	"cloudflow/sdk/golang/cloudflow/kvops"
)

func StartWorker(cfg map[string]interface{}, ops kvops.KVOp) {
	imp := cfg["imp"]
	switch imp {
	case "dumy_worker":
		NewDumyWorker(ops).Run()
	default:
		cf.Assert(false, "woker: %s not support", imp)
	}
}

func TryStartWorker(cfg map[string]interface{}, ops kvops.KVOp) {
	sche := cfmodule.ListCfModule(ops, cfmodule.K_CF_WORKERS)
	if len(sche) < 1 {
		cf.Log("no workers find, create new one")
		StartWorker(cfg, ops)
	}
	cf.Log("runing workers:", cfmodule.ListCfModule(ops, cfmodule.K_CF_WORKERS))
}

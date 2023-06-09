package schedule

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/kvops"
)

type SchOps interface {
	Run()
	Sync()
}

const K_SCHEDULERS = "cfschedulers"

func AddScheduler(ops kvops.KVOp, sc *StateSche) bool {
	// Add to sche list
	kvops.Lock(ops, K_SCHEDULERS, sc.Uuid) // locak
	scs := ListScheduler(ops)
	scs = append(scs, sc.Uuid)
	ops.Set(K_SCHEDULERS, scs)
	kvops.UnLock(ops, K_SCHEDULERS, sc.Uuid) // unlock
	// Add scheduler data
	ops.SetKV(cf.Dump(cf.AsKV(sc), "sche."+sc.Uuid, "uuid"), false)
	return true
}

func ListScheduler(ops kvops.KVOp) []string {
	// scope.cfschedulers
	scs := ops.Get(K_SCHEDULERS)
	if scs == nil {
		return []string{}
	}
	ret := []string{}
	for _, v := range scs.([]interface{}) {
		ret = append(ret, v.(string))
	}
	return ret
}

package schedule

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	"cloudflow/sdk/golang/cloudflow/kvops"
)

type DumySche struct {
	cfmodule.StateCfModule
}

func (sch *DumySche) Run() {
	cf.Log("start dumy scheduler:", sch.Name)
	cfmodule.AddCfModule(sch.Kvops, &sch.StateCfModule, cf.AsKV(sch), cfmodule.K_CF_SCHEDUS, cfmodule.K_AB_SCHEDU)
	go func() {
		for {
			// clear un-active, completed tasks
			// watch key + timeout
			// find new tasks mark.sch tag and assigne to worker
		}
	}()
}

func (sch *DumySche) Sync() {}

func NewDumySche(kvops kvops.KVOp) cfmodule.CfModuleOps {
	sche := DumySche{}
	sche.StateCfModule = cfmodule.NewStateCfModule(kvops, "DumySche", "a dumy scheduler")
	return &sche
}

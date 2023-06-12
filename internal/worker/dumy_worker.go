package worker

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"time"
)

type DumyWorker struct {
	Task []string `json:"task"`
	cfmodule.StateCfModule
}

func (wk *DumyWorker) Run() {
	cf.Log("start dumy worker:", wk.Name)
	cfmodule.AddCfModule(wk.Kvops, &wk.StateCfModule, cf.AsKV(wk), cf.K_CF_WORKERS, cf.K_AB_WORKER)
	go func() {
		for {
			// check task queue
			// watch key + timeout
			// find new tasks mark.sch tag and assigne to worker
			time.Sleep(time.Second)
		}
	}()
}

func (wk *DumyWorker) Sync() {}

func NewDumyWorker(kvops kvops.KVOp) cfmodule.CfModuleOps {
	worker := DumyWorker{
		Task: []string{},
	}
	worker.StateCfModule = cfmodule.NewStateCfModule(kvops, "DumyWorker", "a dumy worker")
	return &worker
}

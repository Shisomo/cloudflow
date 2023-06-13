package worker

import (
	"cloudflow/internal/task"
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"strings"
	"time"
)

type DumyWorker struct {
	cfmodule.StateCfModule
}

func (wk *DumyWorker) Run() {
	cf.Log("start dumy worker:", wk.Name)
	cfmodule.AddModuleAndToList(wk.Kvops, wk.StateCfModule.Uuid, cf.AsKV(wk),
		cf.K_CF_WORKERS, cf.K_STAT_WORK, cf.K_AB_WORKER)
	go func() {
		for {
			// check task queue
			tasks := ListTasks(wk.Kvops, wk.Uuid)
			cf.Log("find launch tasks:", len(tasks))
			// watch key + timeout
			// find new tasks mark.sch tag and assigne to worker
			time.Sleep(5 * time.Second)
			for _, tsk := range tasks {
				if strings.Contains(tsk.Uuid_key, "-") {
					cf.Log("regject task:", tsk.Uuid_key)
					wk.Kvops.Del(cf.DotS(cf.K_AB_WORKER, wk.Uuid, cf.K_AB_TASK, tsk.Uuid_key))
					task.UpTaskStat(wk.Kvops, tsk, cf.K_STAT_WAIT, wk.StateCfModule.Uuid)
				}
			}
			time.Sleep(5 * time.Second)
		}
	}()
}

func RunTask(kvops, task task.Task) {
	// pass
}

func NewDumyWorker(kvops kvops.KVOp) cfmodule.CfModuleOps {
	worker := DumyWorker{}
	worker.StateCfModule = cfmodule.NewStateCfModule(kvops, "DumyWorker", "a dumy worker")
	return &worker
}

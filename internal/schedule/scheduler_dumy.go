package schedule

import (
	"cloudflow/internal/task"
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"runtime"
	"time"
)

type DumySche struct {
	cfmodule.StateCfModule
}

func (sch *DumySche) Run() {
	cf.Log("start dumy scheduler:", sch.Name)
	cfmodule.AddModuleAndToList(sch.Kvops, sch.StateCfModule.Uuid, cf.AsKV(sch),
		cf.K_CF_SCHEDUS, cf.K_STAT_WORK, cf.K_AB_SCHEDU)

	go func() {
		for {
			// get all workers
			workers := cfmodule.ListKeys(sch.Kvops, cf.K_CF_WORKERS, cf.K_STAT_WORK)
			worker_size := len(workers)
			if worker_size < 1 {
				cf.Log("No working workers find")
				time.Sleep(5 * time.Second)
				continue
			}

			tasks := task.GetAllTasks(sch.Kvops, cf.K_STAT_WAIT)
			cf.Log("find all", cf.K_STAT_WAIT, "tasks:", len(tasks))

			// copy tasks
			for _, tsk := range tasks {
				stat := cfmodule.GetStat(sch.Kvops, tsk.Uuid_key)
				cf.Assert(stat == cf.K_STAT_WAIT, "task stat not persist: %s != %s", stat, cf.K_STAT_WAIT)
				raw_icount := cfmodule.GetVal(sch.Kvops, tsk.Uuid_key, cf.K_MEMBER_INSCOUNT)
				inst_count := int(raw_icount.(float64)) - 1
				if inst_count < 0 {
					// auto scale: FIXME
					inst_count = runtime.NumCPU() - 1
					cf.Log("auto sacle to", inst_count, "instances")
				}
				if inst_count > 0 {
					if int(cfmodule.GetVal(sch.Kvops, tsk.Uuid_key, cf.K_MEMBER_SUB_INDX).(float64)) == 0 {
						// copy instances
						cf.Log("copy task", tsk.Uuid_key, "as", inst_count, "ones")
						task.CopyTasks(sch.Kvops, tsk, inst_count, cf.K_STAT_WAIT)
					}
				}
			}
			tsk_id := 0
			for _, tsk := range tasks {
				sc_worker := workers[tsk_id%worker_size]
				cf.Log("schedule task:", tsk, " to worker:", sc_worker)
				task.AddTaskTo(sch.Kvops, tsk, sc_worker)
				task.UpTaskStat(sch.Kvops, tsk, cf.K_STAT_PEDD, cf.DotS(cf.K_AB_SCHEDU, sch.Uuid))
				tsk_id += 1
			}
			// watch key + timeout: TBD
			time.Sleep(5 * time.Second)
		}
	}()
}

func NewDumySche(kvops kvops.KVOp) cfmodule.CfModuleOps {
	sche := DumySche{}
	sche.StateCfModule = cfmodule.NewStateCfModule(kvops, "DumySche", "a dumy scheduler")
	return &sche
}

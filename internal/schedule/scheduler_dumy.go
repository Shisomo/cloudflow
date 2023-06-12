package schedule

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"runtime"
	"strings"
	"time"
)

type DumySche struct {
	cfmodule.StateCfModule
}

func (sch *DumySche) Run() {
	cf.Log("start dumy scheduler:", sch.Name)
	cfmodule.AddCfModule(sch.Kvops, &sch.StateCfModule, cf.AsKV(sch), cf.K_CF_SCHEDUS, cf.K_AB_SCHEDU)
	go func() {
		for {
			// get all workers
			workers := cfmodule.ListCfModule(sch.Kvops, cf.K_CF_WORKERS)
			worker_size := len(workers)
			tasks := []string{}
			cf.Assert(worker_size > 0, "No workers find")
			// get executable items: node, service
			for _, app := range cfmodule.ListCfModule(sch.Kvops, cf.K_CF_APPLIST) {
				for _, srv := range cfmodule.ListCfModule(sch.Kvops, cf.DotS(cf.K_AB_CFAPP, app, cf.K_AB_SESSION)) {
					tasks = append(tasks, cf.DotS(cf.K_AB_SERVICE, srv))
				}
				for _, ses := range cfmodule.ListCfModule(sch.Kvops, cf.DotS(cf.K_AB_CFAPP, app, "sess")) {
					for _, flow := range cfmodule.ListCfModule(sch.Kvops, cf.DotS(cf.K_AB_SESSION, ses, "flow")) {
						for _, node := range cfmodule.ListCfModule(sch.Kvops, cf.DotS(cf.K_AB_FLOW, flow, "node")) {
							tasks = append(tasks, cf.DotS("node", node))
						}
					}
				}
			}
			cf.Log("find all tasks:", tasks)
			// check stat, and assigne to worker
			all_tasks := []string{}
			tsk_id := 0
			for _, tsk := range tasks {
				if cfmodule.GetStat(sch.Kvops, tsk) == cf.K_STAT_WAIT {
					inst_count := int(cfmodule.GetVal(sch.Kvops, tsk, cf.K_MEMBER_INSCOUNT).(float64)) - 1
					if inst_count < 0 {
						// auto scale: FIXME
						inst_count = runtime.NumCPU() - 1
					}
					if inst_count > 0 {
						if int(cfmodule.GetVal(sch.Kvops, tsk, cf.K_MEMBER_SUB_INDX).(float64)) == 0 {
							// copy instances
							ins_data := cfmodule.CopyIns(sch.Kvops, tsk, inst_count)
							// sync data to parents
							subkey := strings.Split(tsk, ".")[0]
							cfmodule.AddCfKVlaues(sch.Kvops, ins_data, cf.DotS(tsk, cf.K_MEMBER_PARENT), subkey, sch.Uuid)
						}
					}
					all_tasks = append(all_tasks, tsk)
				}
			}
			cf.Log("tasks to schedule:", len(all_tasks))
			for _, tsk := range all_tasks {
				sc_worker := workers[tsk_id%worker_size]
				sc_tasks := []string{tsk}
				cf.Log("schedule task:", tsk, " to worker:", sc_worker)
				cfmodule.AddToList(sch.Kvops, cf.DotS(cf.K_AB_WORKER, sc_worker), "task", sc_tasks, sch.Uuid)
				cfmodule.UpdateStat(sch.Kvops, tsk, cf.K_STAT_PEDD, sch.Uuid)
				tsk_id += 1
			}
			// watch key + timeout: TBD
			time.Sleep(5 * time.Second)
		}
	}()
}

func (sch *DumySche) Sync() {}

func NewDumySche(kvops kvops.KVOp) cfmodule.CfModuleOps {
	sche := DumySche{}
	sche.StateCfModule = cfmodule.NewStateCfModule(kvops, "DumySche", "a dumy scheduler")
	return &sche
}

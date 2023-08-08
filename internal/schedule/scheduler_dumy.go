package schedule

import (
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"cloudflow/sdk/golang/cloudflow/task"
	"runtime"
	"time"
)

type DumySche struct {
	cfmodule.StateCfModule
}

func (sch *DumySche) Run() {
	cf.Log("start dumy scheduler:", sch.Name)
	// 将sch这个DumySche结构体分析成map并存储进etcd中
	//example: AddModuleAndToList(kvops, cf.uuid, ?askv, "cfschedus", "workong", "sche")
	cfmodule.AddModuleAndToList(sch.Kvops, sch.StateCfModule.Uuid, cf.AsKV(sch),
		cf.K_CF_SCHEDUS, cf.K_STAT_WORK, cf.K_AB_SCHEDU)

	go func() {
		for {
			// get all workers
			// 通过kv实例、cfworkers字段、worker的状态获得所有workers
			// cl.wokr.69fbe8bb07ebb30b26209467436305f4
			workers := cfmodule.ListKeys(sch.Kvops, cf.K_CF_WORKERS, cf.K_STAT_WORK)
			worker_size := len(workers)
			// worker数量小于1，就等待5s重新获取workers
			if worker_size < 1 {
				cf.Log("No working workers find")
				time.Sleep(5 * time.Second)
				continue
			}
			// get all tasks
			// 通过kv实例、等待状态关键字，列出所有处于等待状态的node和service
			tasks := task.GetAllTasks(sch.Kvops, cf.K_STAT_WAIT)
			// task数量小于1说明不存在，继续等待task
			if len(tasks) < 1 {
				time.Sleep(5 * time.Second)
				continue
			}
			cf.Log("find all", cf.K_STAT_WAIT, "tasks:", len(tasks))

			// copy tasks
			// task和worker均大于1 执行该段逻辑

			for _, tsk := range tasks {
				// etcdctl get cl.node.6a684c61e2557ee5159ba6f1b1b80344.cstat
				stat := cfmodule.GetStat(sch.Kvops, tsk.Uuid_key)
				// 判断节点状态，状态不是等待就？？？
				cf.Assert(stat == cf.K_STAT_WAIT, "task stat not persist: %s != %s", stat, cf.K_STAT_WAIT)
				// etcdctl get cl.node.6a684c61e2557ee5159ba6f1b1b80344.inscount
				raw_icount := cfmodule.GetVal(sch.Kvops, tsk.Uuid_key, cf.K_MEMBER_INSCOUNT)

				inst_count := int(raw_icount.(float64)) - 1
				if inst_count < 0 {
					// auto scale: FIXME
					// 将
					inst_count = runtime.NumCPU() - 1
					cf.Log("auto sacle to", inst_count+1, "instances")
				}
				if inst_count > 0 {
					// 如果 cl.node.6a684c61e2557ee5159ba6f1b1b80344.subidx 为 0
					if int(cfmodule.GetVal(sch.Kvops, tsk.Uuid_key, cf.K_MEMBER_SUB_INDX).(float64)) == 0 {
						// copy instances
						cfmodule.SetVal(sch.Kvops, inst_count+1, tsk.Uuid_key, cf.K_MEMBER_INSCOUNT)
						cf.Log("copy task", tsk.Uuid_key, "as", inst_count+1, "ones")
						// cl.node.6a684c61e2557ee5159ba6f1b1b80344-1
						task.CopyTasks(sch.Kvops, tsk, inst_count, cf.K_STAT_WAIT)
					}
				}
			}
			tsk_id := 0
			for _, tsk := range tasks {
				// 简单的负载分配: task数量%取余worker数量
				sc_worker := workers[tsk_id%worker_size]
				cf.Log("schedule task:", tsk, " to worker:", sc_worker)
				// 添加task到etcd中
				// cl.wokr.69fbe8bb07ebb30b26209467436305f4.task.node.693b4e2e385b2e55de47bddf038e8384
				task.AddTo(sch.Kvops, tsk, sc_worker)
				// 更新元数据库状态 pedding
				task.UpdateStat(sch.Kvops, tsk, cf.K_STAT_PEDD, cf.DotS(cf.K_AB_SCHEDU, sch.Uuid))
				tsk_id += 1
			}
			// watch key + timeout: TBD
			if len(tasks) < 1 {
				time.Sleep(5 * time.Second)
			}
		}
	}()
}

func NewDumySche(kvops kvops.KVOp) cfmodule.CfModuleOps {
	sche := DumySche{}
	sche.StateCfModule = cfmodule.NewStateCfModule(kvops, "DumySche", "a dumy scheduler")
	return &sche
}

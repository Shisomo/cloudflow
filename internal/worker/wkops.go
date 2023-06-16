package worker

import (
	"cloudflow/internal/task"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/kvops"
)

func ListTasks(ops kvops.KVOp, worker_uuid string) []task.Task {
	ret := []task.Task{}
	prefix := cf.DotS(cf.K_AB_WORKER, worker_uuid, "task.")
	tasks := ops.Get(prefix + "*")
	if tasks == nil {
		return ret
	}
	for _, t := range tasks.(map[string]interface{}) {
		t := t.(map[string]interface{})
		ret = append(ret, task.Task{
			Uuid_key: t["ukey"].(string),
			List_key: t["lkey"].(string),
		})
	}
	return ret
}

func FilterTaskByStat(ops kvops.KVOp, tasks []task.Task, stat string) []task.Task {
	ret := []task.Task{}
	for _, tsk := range tasks {
		if task.Stat(ops, tsk) != stat {
			continue
		}
		ret = append(ret, tsk)
	}
	return ret
}

func ClearALLTasks(ops kvops.KVOp, worker string) {
	for _, k := range ListTasks(ops, worker) {
		ops.Del(cf.DotS(cf.K_AB_WORKER, worker, "task", k.Uuid_key))
	}
}

package task

import (
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/kvops"
)

type Task struct {
	List_key string `json:"lkey"`
	Uuid_key string `json:"ukey"`
}

func GetAllTasks(ops kvops.KVOp, stat string) []Task {
	tasks := []Task{}
	// get executable items: node, service
	for _, app := range cfmodule.ListKeys(ops, cf.K_CF_APPLIST, "") {
		prefix_srv := cf.DotS(app, cf.K_AB_SERVICE)
		for _, srv := range cfmodule.ListKeys(ops, prefix_srv, stat) {
			tasks = append(tasks, Task{
				List_key: prefix_srv,
				Uuid_key: srv,
			})
		}
		for _, ses := range cfmodule.ListKeys(ops, cf.DotS(app, cf.K_AB_SESSION), "") {
			for _, flow := range cfmodule.ListKeys(ops, cf.DotS(ses, cf.K_AB_FLOW), "") {
				prefix_node := cf.DotS(flow, cf.K_AB_NODE)
				for _, node := range cfmodule.ListKeys(ops, prefix_node, stat) {
					tasks = append(tasks, Task{
						List_key: prefix_node,
						Uuid_key: node,
					})
				}
			}
		}
	}
	return tasks
}

func CopyTasks(ops kvops.KVOp, tsk Task, count int, stat string) {
	ins_data := cfmodule.CopyIns(ops, tsk.Uuid_key, count)
	cfmodule.BatchAddRawDataAndToList(ops, ins_data, tsk.List_key, stat)
}

func UpdateStat(ops kvops.KVOp, tsk Task, stat string, who string) {
	ops.Set(cf.DotS(tsk.List_key, tsk.Uuid_key), stat)
	cfmodule.UpdateStat(ops, tsk.Uuid_key, stat, who)
}

func Stat(ops kvops.KVOp, tsk Task) string {
	return cfmodule.GetStat(ops, tsk.Uuid_key)
}

func AddTo(ops kvops.KVOp, tsk Task, worker string) {
	ops.Set(cf.DotS(worker, cf.K_AB_TASK, tsk.Uuid_key), tsk)
}

func NodesState(ops kvops.KVOp, node_key_uuid string, target_stat string) {
	ops.Get(node_key_uuid + "*")
}
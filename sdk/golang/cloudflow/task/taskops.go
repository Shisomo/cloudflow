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
	// 遍历前缀为cfapplist的值为空的切片[]
	// cl.cfapplist.cfapp.d538effc0a709ca520030e04f77b1d90
	for _, app := range cfmodule.ListKeys(ops, cf.K_CF_APPLIST, "") {
		prefix_srv := cf.DotS(app, cf.K_AB_SERVICE)
		// 遍历前缀为cfapp.srvs的所有状态为stat的服务
		for _, srv := range cfmodule.ListKeys(ops, prefix_srv, stat) {
			tasks = append(tasks, Task{
				List_key: prefix_srv,
				Uuid_key: srv,
			})
		}
		// 遍历前缀为cfapp.sess的所有无状态的session切片
		// cl.cfapp.d538effc0a709ca520030e04f77b1d90.sess.sess.2301f5ca1377bea4886c51fa3904b644
		for _, ses := range cfmodule.ListKeys(ops, cf.DotS(app, cf.K_AB_SESSION), "") {
			// 遍历前缀为sess.uuid.flow的所有无状态的flow切片
			// cl.sess.2301f5ca1377bea4886c51fa3904b644.flow.flow.f6d5ac2649aba0240d8a88f1d6423e85
			for _, flow := range cfmodule.ListKeys(ops, cf.DotS(ses, cf.K_AB_FLOW), "") {
				prefix_node := cf.DotS(flow, cf.K_AB_NODE)
				// 遍历前缀为flow_key.node的所有状态为stat的节点切片
				// cl.flow.f6d5ac2649aba0240d8a88f1d6423e85.node.node.6a684c61e2557ee5159ba6f1b1b80344
				for _, node := range cfmodule.ListKeys(ops, prefix_node, stat) {
					tasks = append(tasks, Task{
						List_key: prefix_node, // flow.f6d5ac2649aba0240d8a88f1d6423e85.node
						Uuid_key: node,        // node.6a684c61e2557ee5159ba6f1b1b80344
					})
				}
			}
		}
	}
	// 将所有状态为stat的服务srvs或节点node均加入tasks中
	// ListKey会去除前缀，只保留uuid
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

func ListTasks(ops kvops.KVOp, worker_uuid string) []Task {
	ret := []Task{}
	prefix := cf.DotS(cf.K_AB_WORKER, worker_uuid, "task.")
	tasks := ops.Get(prefix + "*")
	if tasks == nil {
		return ret
	}
	for _, t := range tasks.(map[string]interface{}) {
		t := t.(map[string]interface{})
		ret = append(ret, Task{
			Uuid_key: t["ukey"].(string),
			List_key: t["lkey"].(string),
		})
	}
	return ret
}

func FilterTaskByStat(ops kvops.KVOp, tasks []Task, stat string) []Task {
	ret := []Task{}
	for _, tsk := range tasks {
		if Stat(ops, tsk) != stat {
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

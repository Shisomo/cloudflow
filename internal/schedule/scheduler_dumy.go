package schedule

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"cloudflow/sdk/golang/cloudflow/schedule"
)

type DumySche struct {
	schedule.StateSche
}

func (sch *DumySche) Run() {
	cf.Log("start dumy scheduler:", sch.Name)
	schedule.AddScheduler(sch.Kvops, &sch.StateSche)
	go func() {
		for {
			// clear un-active, completed tasks
			// watch key + timeout
			// find new tasks mark.sch tag and assigne to worker
		}
	}()
}

func (sch *DumySche) Sync() {}

func NewDumySche(kvops kvops.KVOp) schedule.SchOps {
	sche := DumySche{}
	sche.StateSche = schedule.StateSche{
		Kvops: kvops,
		Name:  "DumyScheduler-" + cf.AsMd5(cf.TimestampStr()),
		Uuid:  cf.AsMd5(cf.AppID() + cf.TimestampStr()),
		CTime: cf.Timestamp(),
		CommStat: comm.CommStat{
			Descr: "a dumy scheduler",
			Host:  cf.NodeID(),
		},
	}
	return &sche
}

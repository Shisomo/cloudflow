package cfmodule

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/kvops"
)

type StateCfModule struct {
	Uuid  string     `json:"uuid"`
	Name  string     `json:"name"`
	CTime int64      `json:"ctime"`
	Kvops kvops.KVOp `json:"-"`
	comm.CommStat
}

func NewStateCfModule(kvops kvops.KVOp, name string, desc string) StateCfModule {
	return StateCfModule{
		Kvops: kvops,
		Name:  name + "-" + cf.AsMd5(cf.TimestampStr()),
		Uuid:  cf.AsMd5(cf.AppID() + name + cf.TimestampStr()),
		CTime: cf.Timestamp(),
		CommStat: comm.CommStat{
			Descr: desc,
			Host:  cf.NodeID(),
		},
	}
}

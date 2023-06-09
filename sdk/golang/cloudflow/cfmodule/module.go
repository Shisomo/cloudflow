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

const K_CF_SCHEDUS = "cfschedus"
const K_CF_WORKERS = "cfworkers"
const K_CF_APPLIST = "cfapplist"

const K_AB_WORKER = "wokr"
const K_AB_SCHEDU = "sche"
const K_AB_CFAPP = "cfapp"
const K_AB_SERVICE = "srvs"
const K_AB_SESSION = "sess"
const K_AB_FLOW = "flow"
const K_AB_NODE = "node"

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

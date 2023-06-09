package schedule

import (
	"cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/kvops"
)

type StateSche struct {
	Uuid  string     `json:"uuid"`
	Name  string     `json:"name"`
	CTime int64      `json:"ctime"`
	Kvops kvops.KVOp `json:"-"`
	comm.CommStat
}

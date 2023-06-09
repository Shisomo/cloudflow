package worker

import (
	comm "cloudflow/sdk/golang/cloudflow/comm"
)

type StateWorker struct {
	Uuid  string `json:"uuid"`
	Name  string `json:"name"`
	CTime int64  `json:"ctime"`
	comm.CommStat
}

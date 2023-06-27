package cloudflow

import (
	"cloudflow/sdk/golang/cloudflow/chops"
	"cloudflow/sdk/golang/cloudflow/kvops"
)

type RunInterface interface {
	Exited() bool
	StartCall()
	PreCall()
	Call(args []interface{}) []interface{}
	SyncState()
	Exit(reason string)
	InOutChs() ([]string, []string)
	SetSubIdx(idx int)
	SetKVOps(ops kvops.KVOp)
	SetMsgOps(ops chops.ChannelOp)
	MsgLog(a ...interface{})
	MsgLogf(fmt string, a ...interface{})
	FuncName() string
	InstanceCount() int
	GetBatchSize() int
	GetExitChs() (map[string][]interface{}, bool)
	UpdateUUID(node_key string)
	UUID() string
}

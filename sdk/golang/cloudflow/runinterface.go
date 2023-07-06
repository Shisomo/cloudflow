package cloudflow

import (
	"cloudflow/sdk/golang/cloudflow/chops"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"cloudflow/sdk/golang/cloudflow/task"
)

type RunInterface interface {
	GetName() string
	SyncState()
	SetSubIdx(idx int)
	SetKVOps(ops kvops.KVOp)
	SetMsgOps(ops chops.ChannelOp)
	MsgLog(a ...interface{})
	MsgLogf(fmt string, a ...interface{})
	FuncName() string
	InstanceCount() int
	UpdateUUID(node_key string)
	UUID() string
	CallCount() int64
	AsTask() task.Task
	Run() int64
}

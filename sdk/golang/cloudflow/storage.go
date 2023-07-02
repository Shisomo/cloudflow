package cloudflow

import (
	"bytes"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/fileops"
	"cloudflow/sdk/golang/cloudflow/kvops"
)

type FileOps interface{}

type StorageOps interface {
	KvDel(key string)
	KvDels(key []string)
	KvGet(key string) interface{}
	KvSet(key string, value interface{})
	KvGets(keys []string) map[string]interface{}
	KvSets(data map[string]interface{})
	FileOpen(filename string, ops ...FileOps) bytes.Buffer
	FileFlush(filename string)
	FileClose(filename string)
	FileDel(filename string)
	FileExists(filename string) bool
	Scope() string
}

type Storage struct {
	scope string
	kv    kvops.KVOp
	file  fileops.FileOps
}

func NewStorage(scope string, root_cfg *cf.CFG) StorageOps {
	return nil
}

package cloudflow

import (
	"bytes"
	"cloudflow/sdk/golang/cloudflow/comm"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/fileops"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"os"
)

type FileOps interface{}

type StorageOps interface {
	KvDel(key string)
	KvDels(key []string)
	KvGet(key string) interface{}
	KvSet(key string, value interface{})
	KvGets(keys []string) map[string]interface{}
	KvSets(data map[string]interface{})
	FileOpen(filename string) bytes.Buffer
	FileWrite(filename string, buf bytes.Buffer)
	fileFlush(filename string)
	FileClose(filename string)
	FileDel(filename string)
	FileExists(filename string) bool
	Scope() string
}

// NewStorage 用于创建存储操作对象
func NewStorage(session *Session, root_cfg *cf.CFG, r RunInterface, ops ...interface{}) StorageOps {
	// TBD
	scope := session.Name
	fileOps := fileops.GetFileOps(scope, *root_cfg)
	(*root_cfg)["scope"] = scope
	kvOps := kvops.GetKVOpImp(*root_cfg)
	return &Storage{
		scope:   scope,
		kv:      kvOps,
		file:    fileOps,
		Session: session,
		Node:    r,
	}

}

type Storage struct {
	ID      string
	scope   string
	Session *Session
	kv      kvops.KVOp
	file    fileops.FileOps
	Node    RunInterface
	cf.CommStat
}

// 删除以key为键的键值对
func (storage *Storage) KvDel(key string) {
	if storage.kv == nil {
		cf.Info("kv delete failed: session %s kv not exist\n", storage.scope)
		return
	}
	storage.kv.Del(key)
}

// 删除所有以key列表为键的所有键值对
func (storage *Storage) KvDels(key []string) {
	if storage.kv == nil {
		cf.Info("kvs delete failed: session %s kv not exist\n", storage.scope)
		return
	}
	for _, v := range key {
		storage.kv.Del(v)
	}
}

// 从storage的kv中查询key的键值，返回它的值
func (storage *Storage) KvGet(key string) interface{} {
	if storage.kv == nil {
		cf.Info("kv get failed: session %s kv not exist\n", storage.scope)
		return nil
	}
	value := storage.kv.Get(key)
	return value
}

// 从storage的kv中查询多个键值，以map类型返回
func (storage *Storage) KvGets(key []string) map[string]interface{} {
	if storage.kv == nil {
		cf.Info("kvs get failed: session %s kv not exist\n", storage.scope)
		return nil
	}
	ignore_empty := true
	ret := storage.kv.GetKs(key, ignore_empty)
	return ret
}
func (storage *Storage) KvSet(key string, value interface{}) {
	if storage.kv == nil {
		cf.Info("kv set failed: session %s kv not exist\n", storage.scope)
		return
	}
	cf.Assert(storage.kv.Set(key, value), "kv set failed\n")

}
func (storage *Storage) KvSets(data map[string]interface{}) {
	if storage.kv == nil {
		cf.Info("kvs set failed: session %s kv not exist\n", storage.scope)
		return
	}
	ignore_empty := true
	storage.kv.SetKV(data, ignore_empty)
}

// 从共享存储中下载文件并打开，没有则创建；通过buffer返回文件内容
func (storage *Storage) FileOpen(filename string) bytes.Buffer {
	buffer := bytes.NewBufferString("")
	// 判断storage是否连接
	if storage.file == nil {
		cf.Info("file open failed: session %s file storage not exist\n", storage.scope)
		return *buffer
	}
	// 判断文件是否存在, 不存在的话创建本地文件，并返回buffer用于文件操作
	if !storage.FileExists(filename) {
		storage.unlock(filename)
		f, err := os.Create(filename)
		comm.Assert(err == nil, "Create Temp file %s fail:%s", f.Name(), err)
		storage.fileFlush(filename)
		storage.KvSet(cf.DotS(storage.scope, filename, "owner"), storage.Session.Name)
		// 设置文件状态
		return *buffer
	}
	// 存在的话，从共享存储中下载文件，并返回包含文件内容的buffer
	// if）fileops
	storage.file.Get(filename, filename)
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0777)
	defer f.Close()
	comm.Assert(err == nil, "open file %s fail:%s", filename, err)
	bufferReadNum, err := buffer.ReadFrom(f)
	comm.Assert(err == nil, "(%d) string Read From file fail: %s", bufferReadNum, err)
	return *buffer

}

// 将文件打开的buf内容写入本地文件
func (storage *Storage) FileWrite(filename string, buf bytes.Buffer) {
	if storage.file == nil {
		cf.Info("file write failed: session %s file storage not exist\n", storage.scope)
	}
	if storage.FileExists(filename) {
		if storage.isFileUsing(filename) {
			return
		}
		storage.lock(filename)
		f, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0777)
		comm.Assert(err == nil, "open sorted file fail")
		buf.WriteTo(f)
		defer func() {
			f.Close()
			storage.unlock(filename)
		}()
	}
}

// 将本地文件与共享文件同步
// 将buffer内容与文件同步并上传共享存储，同时删除本地文件
func (storage *Storage) fileFlush(filename string) {
	if storage.isFileUsing(filename) {
		cf.Info("flush err, file is using:", filename)
		return
	}
	storage.lock(filename)
	cf.Assert(storage.file.Put(filename, filename),
		"upload sort file: %s to store fail", filename)
	storage.unlock(filename)

}

// 与共享文件同步并删除本地文件
func (storage *Storage) FileClose(filename string) {
	storage.fileFlush(filename)
	defer func() {
		os.RemoveAll(filename)
		storage.unlock(filename)
	}()
}
func (storage *Storage) FileDel(filename string) {

}

// 文件是否存在于nats object中
func (storage *Storage) FileExists(filename string) bool {
	return storage.file.Exists(filename)
}

// 获取storage的scope描述
func (storage *Storage) Scope() string {
	return storage.scope
}

func (storage *Storage) isFileUsing(filename string) bool {
	return storage.KvGet(cf.DotS(storage.scope, filename, "lock", "stat")) != cf.STORAGE_FILE_STAT_OPEN
}

// time
// owner
//
// value map
func (storage *Storage) lock(filename string) {
	storage.KvSet(cf.DotS(storage.scope, filename, "lock", "stat"), cf.STORAGE_FILE_STAT_CLOSE)
	storage.KvSet(cf.DotS(storage.scope, filename, "lock", "owner"), storage.Node.UUID())
}
func (storage *Storage) unlock(filename string) {
	storage.KvSet(cf.DotS(storage.scope, filename, "lock", "stat"), cf.STORAGE_FILE_STAT_OPEN)
	storage.KvSet(cf.DotS(storage.scope, filename, "lock", "owner"), "")
}

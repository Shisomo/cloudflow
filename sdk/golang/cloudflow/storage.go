package cloudflow

import (
	"bytes"
	"cloudflow/sdk/golang/cloudflow/comm"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/fileops"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"fmt"
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
	FileFlush(filename string, buf bytes.Buffer)
	FileClose(filename string)
	FileDel(filename string)
	FileExists(filename string) bool
	Scope() string
}

func NewStorage(scope string, root_cfg *cf.CFG, ops ...interface{}) StorageOps {
	// TBD
	fileOps := fileops.GetFileOps(scope, *root_cfg)
	// kvOps := kvops.GetKVOpImp(*root_cfg)
	return &Storage{
		scope: scope,
		kv:    nil,
		file:  fileOps,
	}

}

type Storage struct {
	scope string
	kv    kvops.KVOp
	file  fileops.FileOps
}

func (storage *Storage) KvDel(key string) {
}
func (storage *Storage) KvDels(key []string) {
}
func (storage *Storage) KvGet(key string) interface{} {
	return ""
}
func (storage *Storage) KvGets(key []string) map[string]interface{} {
	return map[string]interface{}{}
}
func (storage *Storage) KvSet(key string, value interface{}) {
}
func (storage *Storage) KvSets(data map[string]interface{}) {
}

// 从共享存储中下载文件并打开，没有则创建；通过buffer返回文件内容
func (storage *Storage) FileOpen(filename string) bytes.Buffer {
	buffer := bytes.NewBufferString("")
	// 判断storage是否连接
	if storage.file != nil {
		// 判断文件是否存在于本地, 不存在的话创建本地文件，并返回buffer用于文件操作
		if !storage.file.Exists(filename) {
			f, err := os.Create(filename)
			comm.Assert(err == nil, "Create Temp file %s fail:%s", f.Name(), err)
			return *buffer
		}
		// 存在的话，从共享存储中下载文件，并返回包含文件内容的buffer
		storage.file.Get(filename, filename)
		f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0777)
		comm.Assert(err == nil, "write to local file %s fail:%s", filename, err)
		bufferReadNum, err := buffer.ReadFrom(f)
		comm.Assert(err == nil, "(%d) string Read From file fail: %s", bufferReadNum, err)
		f.Close()
		return *buffer
	}
	fmt.Printf("file open failed: session %s file storage not exist\n", storage.scope)
	return *buffer
}

// 将本地文件与共享文件同步
// 将buffer内容与文件同步并上传共享存储，同时删除本地文件
func (storage *Storage) FileFlush(filename string, buf bytes.Buffer) {
	if storage.file != nil {
		// 存在执行取逻辑
		if storage.FileExists(filename) {
			f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0777)
			comm.Assert(err == nil, "open sorted file fail")
			buf.WriteTo(f)
			defer func() {
				f.Close()
				storage.FileClose(filename)
			}()
		}
		cf.Assert(storage.file.Put(filename, filename),
			"upload sort file: %s to store fail", filename)
	}
}

// 删除本地文件
func (storage *Storage) FileClose(filename string) {
	defer func() {
		os.RemoveAll(filename)
	}()
}
func (storage *Storage) FileDel(filename string) {

}
func (storage *Storage) FileExists(filename string) bool {
	if _, err := os.Stat(filename); err != nil {
		return false
	}
	return true
}

// 获取storage的scope描述
func (storage *Storage) Scope() string {
	return storage.scope
}

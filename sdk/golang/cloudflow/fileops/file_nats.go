package fileops

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"

	"github.com/nats-io/nats.go"
)

type FileOpsNats struct {
	connurl string
	nc      *nats.Conn
	js      nats.JetStreamContext
	scope   string
}

func (self *FileOpsNats) Close() {
	self.nc.Close()
}

func (self *FileOpsNats) Conn() bool {
	nc, err := nats.Connect(self.connurl)
	cf.Assert(err == nil, "connet Nats error: %s", err)
	js, err := nc.JetStream()
	cf.Assert(err == nil, "Create JetStream Error: %s", err)
	self.nc = nc
	self.js = js
	return true
}

func (self *FileOpsNats) List() []string {
	keys, err := self.getKV().Keys()
	cf.Assert(err == nil, "list key fail: %s", err)
	return keys
}

func (self *FileOpsNats) Exists(key string) bool {
	_, err := self.getKV().Get(key)
	if err != nil {
		cf.Log("exists key ", key, "check fail , error: ", err)
		return false
	}
	return true
}

func (self *FileOpsNats) Del(key string) bool {
	f, e := self.getKV().Get(key)
	if e != nil {
		cf.Log("get key fail: ", key)
		return false
	}
	err := self.getObject().Delete(string(f.Value()))
	if err != nil {
		cf.Log("Delete file fail: ", err)
		return false
	}
	return true
}

func (self *FileOpsNats) Put(key string, file_path string) bool {
	cf.Log("upload file:", file_path)
	info, err := self.getObject().PutFile(file_path)
	if err != nil {
		cf.Log("upload fail:", err)
		return false
	}
	self.getKV().Put(key, []byte(file_path))
	cf.Log(cf.ByteHuman(float64(info.Size)), " uploaded")
	return true
}

func (self *FileOpsNats) Get(key string, target_path string) bool {
	object := self.getObject()
	val, err := self.getKV().Get(key)
	if err != nil {
		lis, _ := self.getObject().List()
		for _, v := range lis {
			cf.Log(">>>>", v)
		}
		cf.Assert(err == nil, "get key(%s) error:%s", key, err)
		return false
	}
	err = object.GetFile(string(val.Value()), target_path)
	cf.Assert(err == nil, "get file fail: %s", err)
	return true
}

func (self *FileOpsNats) Clear() bool {
	// FIXME: need delete all file/kv data
	return true
}

func (self *FileOpsNats) getObject() nats.ObjectStore {
	obj, err := self.js.ObjectStore(cf.AsMd5(self.scope) + "FS")
	if err == nil {
		return obj
	}
	newobj, err := self.js.CreateObjectStore(&nats.ObjectStoreConfig{
		Bucket:      cf.AsMd5(self.scope) + "FS",
		Description: "ObjectSotr for: " + self.scope,
		Storage:     nats.FileStorage,
		Replicas:    1,
		Placement:   &nats.Placement{Cluster: "default"},
		MaxBytes:    int64(10e9),
	})
	cf.Assert(err == nil, "create ObjectStore(%s) error: %s", self.scope, err)
	return newobj
}

func (self *FileOpsNats) getKV() nats.KeyValue {
	kv, err := self.js.KeyValue(cf.AsMd5(self.scope) + "KV")
	if err == nil {
		return kv
	}
	newkey, err := self.js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:      cf.AsMd5(self.scope) + "KV",
		Description: "KV for: " + self.scope,
		Storage:     nats.FileStorage,
		Replicas:    1,
		Placement:   &nats.Placement{Cluster: "default"},
		MaxBytes:    int64(10e8),
	})
	cf.Assert(err == nil, "create KV(%s) error: %s", self.scope, err)
	return newkey
}

func NewFileOpsNats(cnn_url string, scope string) *FileOpsNats {
	nc, err := nats.Connect(cnn_url)
	cf.Assert(err == nil, "connet Nats error: %s", err)
	js, err := nc.JetStream()
	cf.Assert(err == nil, "connet Nats jet stream error: %s", err)
	return &FileOpsNats{
		connurl: cnn_url,
		nc:      nc,
		js:      js,
		scope:   scope,
	}
}

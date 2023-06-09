package cfmodule

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/kvops"
)

type CfModuleOps interface {
	Run()
	Sync()
}

func AddCfModule(ops kvops.KVOp, md *StateCfModule, md_ky_data interface{}, md_key string, md_abr string) bool {
	// cf.md_key: [id1, id2, ...]
	// Add m.uuid to list
	kvops.Lock(ops, md_key, md.Uuid) // locak
	scs := ListCfModule(ops, md_key)
	scs = append(scs, md.Uuid)
	ops.Set(md_key, scs)
	ctime_key := md_key + ".ctime"
	if ops.Get(ctime_key) == nil {
		ops.Set(ctime_key, cf.Timestamp())
	}
	kvops.UnLock(ops, md_key, md.Uuid) // unlock
	// Add data of m
	ops.SetKV(cf.Dump(md_ky_data, md_abr+"."+md.Uuid, "uuid"), false)
	return true
}

func ListCfModule(ops kvops.KVOp, md_key string) []string {
	// cf.md_key: [id1, id2, ...]
	scs := ops.Get(md_key)
	if scs == nil {
		return []string{}
	}
	ret := []string{}
	for _, v := range scs.([]interface{}) {
		ret = append(ret, v.(string))
	}
	return ret
}

func FilterModule(ops kvops.KVOp, keylist []string, abb string, filter_key string, fc func(interface{}) bool) []string {
	ret := []string{}
	for _, k := range keylist {
		target := abb + "." + k + "." + filter_key
		if fc(ops.Get(target)) {
			ret = append(ret, k)
		}
	}
	return ret
}

func RmFromList(ops kvops.KVOp, key string, val []string, uuid_who string) {
	// lock
	kvops.Lock(ops, key, uuid_who)
	target := []string{}
	for _, v := range ops.Get(key).([]string) {
		if cf.StrListHas(val, v) {
			continue
		}
		target = append(target, v)
	}
	ops.Set(key, target)
	// unlock
	kvops.UnLock(ops, key, uuid_who)
}

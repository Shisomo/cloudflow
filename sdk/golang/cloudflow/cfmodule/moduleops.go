package cfmodule

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"strings"
)

type CfModuleOps interface {
	Run()
	Sync()
}

func AddCfModule(ops kvops.KVOp, md *StateCfModule, md_ky_data interface{}, md_key string, md_abr string) bool {
	// cf.md_key: [id1, id2, ...]
	// Add m.uuid to list
	ListAdd(ops, md_key, md.Uuid, md.Uuid, true)
	// Add data of m
	ops.SetKV(cf.Dump(md_ky_data, md_abr+"."+md.Uuid, "uuid"), false)
	return true
}

func AddCfKVlaues(ops kvops.KVOp, uuid_map map[string]interface{}, key string, subfix string, uuid_who string) {
	kvops.Lock(ops, key, uuid_who) // locak
	rkey := key
	if subfix != "" {
		rkey = rkey + "." + subfix
	}
	ulist := ListCfModule(ops, rkey)
	for k, v := range uuid_map {
		ulist = append(ulist, k)
		ops.SetKV(v.(map[string]interface{}), false)
	}
	ops.Set(rkey, ulist)
	NoLockUpdateAcc(ops, key, uuid_who)
	kvops.UnLock(ops, key, uuid_who) // unlock
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

func ListAdd(ops kvops.KVOp, key string, target string, uuid_who string, lock bool) {
	if lock {
		kvops.Lock(ops, key, uuid_who)
	}
	md_list := ListCfModule(ops, key)
	md_list = append(md_list, target)
	ops.Set(key, md_list)
	ctime_key := key + ".ctime"
	if ops.Get(ctime_key) == nil {
		ops.Set(ctime_key, cf.Timestamp())
	}
	NoLockUpdateAcc(ops, key, uuid_who)
	if lock {
		kvops.UnLock(ops, key, uuid_who)
	}
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

func RmFromList(ops kvops.KVOp, key string, subfix string, val []string, uuid_who string) {
	// lock
	kvops.Lock(ops, key, uuid_who)
	rkey := key
	if subfix != "" {
		rkey = rkey + "." + subfix
	}
	target := []string{}
	for _, v := range ops.Get(rkey).([]string) {
		if cf.StrListHas(val, v) {
			continue
		}
		target = append(target, v)
	}
	NoLockUpdateAcc(ops, key, uuid_who)
	ops.Set(rkey, target)
	// unlock
	kvops.UnLock(ops, key, uuid_who)
}

func AddToList(ops kvops.KVOp, key string, subfix string, val []string, uuid_who string) {
	// lock
	kvops.Lock(ops, key, uuid_who)
	rkey := key
	if subfix != "" {
		rkey = rkey + "." + subfix
	}
	target := []string{}
	origno := ops.Get(rkey)
	cf.Assert(origno != nil, "%s", rkey)
	origns := []string{}
	for _, v := range origno.([]interface{}) {
		origns = append(origns, v.(string))
	}
	for _, v := range val {
		if cf.StrListHas(origns, v) {
			continue
		}
		target = append(target, v)
	}
	NoLockUpdateAcc(ops, key, uuid_who)
	ops.Set(rkey, target)
	// unlock
	kvops.UnLock(ops, key, uuid_who)
}

func GetVal(ops kvops.KVOp, key string, sub string) interface{} {
	return ops.Get(key + "." + sub)
}

func GetStat(ops kvops.KVOp, key string) string {
	v := GetVal(ops, key, "cstat")
	if v == nil {
		return ""
	}
	return v.(string)
}

func UpdateStat(ops kvops.KVOp, key string, new_stat string, uuid_who string) {
	kvops.Lock(ops, key, uuid_who)
	ops.Set(key+".cstat", new_stat)
	kvops.UnLock(ops, key, uuid_who)
}

func UpdateAcc(ops kvops.KVOp, key string, uuid_who string) {
	kvops.Lock(ops, key, uuid_who)
	NoLockUpdateAcc(ops, key, uuid_who)
	kvops.UnLock(ops, key, uuid_who)
}

func NoLockUpdateAcc(ops kvops.KVOp, key string, uuid_who string) {
	ops.Set(key+".ctime", cf.Timestamp())
	ops.Set(key+".whoac", uuid_who)
}

func CopyIns(ops kvops.KVOp, key string, count int) map[string]interface{} {
	new_ins := map[string]interface{}{}
	old_ins := ops.Get(key + "*").(map[string]interface{})

	_, r := old_ins[key+"."+cf.K_MEMBER_INSCOUNT]
	cf.Assert(r == false, "%s has no memeber: %s", key, cf.K_MEMBER_INSCOUNT)
	for i := 0; i < count; i++ {
		ins := map[string]interface{}{}
		uid := ""
		// copy values
		for k, v := range old_ins {
			value := v
			// 0     1   2    3
			// scope.abb.uuid.memb
			sp_key := strings.Split(k, ".")
			if uid == "" {
				uid = sp_key[2] + "-" + cf.Itos(i+1)
			}
			cf.Assert(uid != "", "error")
			if len(sp_key) == 4 {
				if sp_key[3] == cf.K_MEMBER_SUB_INDX {
					value = i + 1
				}
			}
			sp_key[2] = uid
			key = strings.Join(sp_key, ".")
			ins[key] = value
		}
		new_ins[uid] = ins
	}
	return new_ins
}

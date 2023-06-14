package cfmodule

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"strings"
)

type CfModuleOps interface {
	Run()
}

func AddModuleAndToList(ops kvops.KVOp, md_uuid string, md_ky_data interface{},
	list_prefix string, stat string, md_abr string) {
	md_key := cf.DotS(md_abr, md_uuid)
	ops.Set(cf.DotS(list_prefix, md_key), stat)
	ops.SetKV(cf.Dump(md_ky_data, md_key, "uuid", "cstat"), false)
}

func BatchAddRawDataAndToList(ops kvops.KVOp, raw_data []interface{}, prefix string, val string) {
	for _, d := range raw_data {
		d := d.(map[string]interface{})
		uuid_key := ""
		for k := range d {
			uuid_key = strings.Join(strings.Split(k, ".")[:2], ".")
			break
		}
		ops.Set(cf.DotS(prefix, uuid_key), val)
		ops.SetKV(d, false)
	}
}

func GetVal(ops kvops.KVOp, key ...string) interface{} {
	return ops.Get(cf.DotS(key...))
}

func SetVal(ops kvops.KVOp, val interface{}, key ...string) bool {
	return ops.Set(cf.DotS(key...), val)
}

func GetStat(ops kvops.KVOp, key string) string {
	v := GetVal(ops, key, "cstat")
	if v == nil {
		return ""
	}
	return v.(string)
}

func UpdateStat(ops kvops.KVOp, key string, new_stat string, uuid_who string) {
	ops.Set(key+".cstat", new_stat)
}

func UpdateAcc(ops kvops.KVOp, key string, uuid_who string) {
	ops.Set(key+".ctime", cf.Timestamp())
	ops.Set(key+".whoac", uuid_who)
}

func CopyIns(ops kvops.KVOp, key string, count int) []interface{} {
	new_ins := []interface{}{}
	old_ins := ops.Get(key + "*").(map[string]interface{})
	key_inscount := cf.DotS(key, cf.K_MEMBER_INSCOUNT)
	_, r := old_ins[key_inscount]
	cf.Assert(r == true, "%s has: %s, cannot copy!, data:%s\n", key, key_inscount, old_ins)
	for i := 0; i < count; i++ {
		n_ins := map[string]interface{}{}
		uid := ""
		// copy values
		for k, v := range old_ins {
			value := v
			// 0     1   2
			// abb.uuid.memb
			sp_key := strings.Split(k, ".")
			if uid == "" {
				uid = sp_key[1] + "-" + cf.Itos(i+1)
			}
			cf.Assert(uid != "", "error")
			if len(sp_key) == 3 {
				if sp_key[2] == cf.K_MEMBER_SUB_INDX {
					value = i + 1
				}
			}
			sp_key[1] = uid
			n_ins[cf.DotS(sp_key...)] = value
		}
		new_ins = append(new_ins, n_ins)
	}
	return new_ins
}

func ListKeys(ops kvops.KVOp, prefix string, targt string) []string {
	//  k.uuid0 val0
	//  k.uuid1 val1
	//  k.uuid2 val2
	//  return: uuid
	ret := []string{}
	values := ops.Get(prefix + ".*")
	if values == nil {
		return ret
	}
	for k, v := range values.(map[string]interface{}) {
		v := v.(string)
		if targt == "" || targt == v {
			ret = append(ret, strings.Replace(k, prefix+".", "", 1))
		}
	}
	return ret
}

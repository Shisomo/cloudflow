package kvops

type KVOp interface {
	Get(key string) interface{}
	Set(key string, value interface{}) bool
	Del(key string) bool
}

func GetKVOpImp(imp string, cfg map[string]interface{}) KVOp {
	return nil
}

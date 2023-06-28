package cloudflow

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"sync"
)

type DataCache struct {
	lock       sync.Mutex
	batch      int
	channes    []string
	dach_cache map[string]*chan string
	dach_defav map[string]string
}

func InitChDataCache(chs []string, batch int) *DataCache {
	dc := DataCache{
		batch:      batch,
		channes:    chs,
		dach_cache: map[string]*chan string{},
		dach_defav: map[string]string{},
	}
	cf.Assert(batch >= 1, "need batch size > 0")
	for _, k := range chs {
		c := make(chan string, 10000*batch)
		dc.dach_cache[k] = &c
	}
	return &dc
}

func (dc *DataCache) SetExitValue(ch_val map[string]string) {
	for k, v := range ch_val {
		dc.dach_defav[k] = v
	}
}

func (dc *DataCache) Put(ch string, data string) {
	dc.lock.Lock()
	*dc.dach_cache[ch] <- data
	dc.lock.Unlock()
}

func (dc *DataCache) Get() ([]interface{}, bool) {
	is_all_dfv := true
	emp := []interface{}{}
	for c, channel := range dc.dach_cache {
		_, has := dc.dach_defav[c]
		if !has {
			if len(*channel) < dc.batch {
				return emp, false
			}
		}
	}
	// batch = 1, return [A, B, C]
	ret := []interface{}{}
	if dc.batch <= 1 {
		for _, ch := range dc.channes {
			queue := dc.dach_cache[ch]
			var data string
			var has bool
			if len(*queue) > 0 {
				data = <-*queue
				is_all_dfv = false
			} else {
				data, has = dc.dach_defav[ch]
				cf.Assert(has, "no default value find for empty queue")
			}
			raw_msg := cf.ParsMsg(data)["app_data"]
			cf.Assert(raw_msg != nil, "app data can not be nil")
			msg := raw_msg.([]interface{})
			ret = append(ret, msg...)
		}
		return ret, is_all_dfv
	}
	// batch > 1, return [A.., B.., C..]
	for _, ch := range dc.channes {
		var items []interface{} // [X..., Y..., Z...]
		item_size := 0
		for i := 0; i < dc.batch; i++ {
			queue := dc.dach_cache[ch]
			var data string
			var has bool
			if len(*queue) > 0 {
				data = <-*queue
				is_all_dfv = false
			} else {
				data, has = dc.dach_defav[ch]
				cf.Assert(has, "no default value find for empty queue")
			}
			msg := cf.ParsMsg(data)["app_data"].([]interface{})
			if item_size == 0 {
				item_size = len(msg)
				items = make([]interface{}, item_size)
			} else {
				cf.Assert(len(msg) == item_size, "message size change")
			}
			for i := 0; i < item_size; i++ {
				if nil == items[i] {
					items[i] = make([]interface{}, dc.batch)
				}
				items[i] = append(items[i].([]interface{}), msg[i])
			}
		}
		ret = append(ret, items...)
	}
	return ret, is_all_dfv
}

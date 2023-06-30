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
	record     bool
	// statistics
	getd_count int64
	getd_sucss int64
	getd_fails int64
	getd_rtime int64
	chan_vsize int64
	chan_msize int64
	chan_ssize int64
	chan_rtime int64
	pars_utime int64
	// ext
	ex_stat map[string]int64
}

func (dc *DataCache) ClearStat() {
	dc.getd_count = 0
	dc.getd_sucss = 0
	dc.getd_fails = 0
	dc.getd_rtime = 0
	dc.chan_vsize = 0
	dc.chan_msize = 0
	dc.chan_ssize = 0
	dc.chan_rtime = 0
	dc.pars_utime = 0
	dc.ex_stat = map[string]int64{}
}

func (dc *DataCache) UpdateExSpeed(key string, stime int64) {
	dc.ex_stat[key] = cf.Timestamp() - stime
}

func (dc *DataCache) Stat() map[string]interface{} {
	return map[string]interface{}{
		"getd_count": dc.getd_count,
		"getd_sucss": dc.getd_sucss,
		"getd_fails": dc.getd_fails,
		"getd_rtime": float64(dc.getd_rtime) / float64(dc.getd_count),
		"chan_vsize": dc.chan_vsize,
		"chan_msize": dc.chan_msize,
		"chan_ssize": dc.chan_ssize,
		"chan_rtime": dc.chan_rtime,
		"pars_utime": dc.pars_utime,
		"ex_stat":    dc.ex_stat,
	}
}

func (dc *DataCache) updateStat(is_sucess bool, start_time int64) {
	if !dc.record {
		return
	}
	dc.getd_count += 1
	if is_sucess {
		dc.getd_sucss += 1
	} else {
		dc.getd_fails += 1
	}
	dc.getd_rtime += cf.Timestamp() - start_time

	all_size := 0
	max_size := 0
	min_size := 100000000000000000
	for _, v := range dc.dach_cache {
		size := len(*v)
		if size < min_size {
			min_size = size
		}
		if size > max_size {
			max_size = size
		}
		all_size += size
	}
	vsize := int64(all_size / len(dc.dach_cache))
	dc.chan_vsize = (dc.chan_vsize*(dc.getd_count-1) + vsize) / dc.getd_count
	dc.chan_msize = int64(max_size)
	dc.chan_ssize = int64(min_size)
}

func InitChDataCache(chs []string, batch int, record bool) *DataCache {
	dc := DataCache{
		batch:      batch,
		channes:    chs,
		dach_cache: map[string]*chan string{},
		dach_defav: map[string]string{},
		record:     record,
		ex_stat:    map[string]int64{},
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
	start_time := cf.Timestamp()
	is_all_dfv := true
	emp := []interface{}{}
	for c, channel := range dc.dach_cache {
		_, has := dc.dach_defav[c]
		if !has {
			if len(*channel) < dc.batch {
				dc.updateStat(false, start_time)
				return emp, false
			}
		}
	}
	// batch = 1, return [A, B, C]
	ret := []interface{}{}
	time_ch_read := int64(0)
	time_ch_cout := int64(0)
	time_dt_pars := int64(0)
	time_dt_cout := int64(0)
	if dc.batch <= 1 {
		for _, ch := range dc.channes {
			queue := dc.dach_cache[ch]
			var data string
			var has bool
			if len(*queue) > 0 {
				cstime := cf.Timestamp()
				data = <-*queue
				time_ch_read += (cf.Timestamp() - cstime)
				time_ch_cout += 1
				is_all_dfv = false
			} else {
				data, has = dc.dach_defav[ch]
				cf.Assert(has, "no default value find for empty queue")
			}
			pstime := cf.Timestamp()
			raw_msg := cf.ParsMsg(data)["app_data"]
			time_dt_pars += (cf.Timestamp() - pstime)
			time_dt_cout += 1
			cf.Assert(raw_msg != nil, "app data can not be nil")
			msg := raw_msg.([]interface{})
			ret = append(ret, msg...)
		}
		dc.updateStat(true, start_time)
		dc.pars_utime = time_dt_pars / cf.Max(time_dt_cout, 1)
		dc.chan_rtime = time_ch_read / cf.Max(time_ch_cout, 1)
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
				cstime := cf.Timestamp()
				is_all_dfv = false
				time_ch_read += (cf.Timestamp() - cstime)
				time_ch_cout += 1
			} else {
				data, has = dc.dach_defav[ch]
				cf.Assert(has, "no default value find for empty queue")
			}
			pstime := cf.Timestamp()
			msg := cf.ParsMsg(data)["app_data"].([]interface{})
			time_dt_pars += (cf.Timestamp() - pstime)
			time_dt_cout += 1
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
	dc.updateStat(true, start_time)
	dc.pars_utime = time_dt_pars / cf.Max(time_dt_cout, 1)
	dc.chan_rtime = time_ch_read / cf.Max(time_ch_cout, 1)
	return ret, is_all_dfv
}

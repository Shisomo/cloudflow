package test

import (
	"cloudflow/sdk/golang/cloudflow/chops"
	"cloudflow/sdk/golang/cloudflow/comm"
	"math/rand"
	"testing"
	"time"
)

func RandStr(length int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

func Test_Ch_performance(t *testing.T) {

	imp := "nats"
	cfg := comm.CFG{
		"app_id": "app_id",
		"host":   "127.0.0.1",
		"port":   4222,
	}
	start_ch := []string{"input_1"}
	inter_ch := []string{"map_1", "map_2", "map_3", "map_4", "map_5", "map_6", "map_7", "map_8", "map_9", "map_10"}
	t.Log("connect ...")
	message := chops.GetChOpsImp(imp, cfg)
	t.Log("start watch")
	message.Watch(start_ch, func(worker, subj, data string) bool {
		message.Put(inter_ch[0:1], data)
		return true
	})
	message.Watch(start_ch, func(worker, subj, data string) bool {
		message.Put(inter_ch[1:2], data)
		return true
	})
	message.Watch(start_ch, func(worker, subj, data string) bool {
		message.Put(inter_ch[2:3], data)
		return true
	})
	message.Watch(start_ch, func(worker, subj, data string) bool {
		message.Put(inter_ch[3:4], data)
		return true
	})
	message.Watch(start_ch, func(worker, subj, data string) bool {
		message.Put(inter_ch[4:5], data)
		return true
	})
	message.Watch(start_ch, func(worker, subj, data string) bool {
		message.Put(inter_ch[5:6], data)
		return true
	})
	message.Watch(start_ch, func(worker, subj, data string) bool {
		message.Put(inter_ch[7:8], data)
		return true
	})
	message.Watch(start_ch, func(worker, subj, data string) bool {
		message.Put(inter_ch[8:9], data)
		return true
	})
	message.Watch(start_ch, func(worker, subj, data string) bool {
		message.Put(inter_ch[9:10], data)
		return true
	})

	total := 0
	recrd := 0
	record_time := comm.Timestamp()
	// reduce
	message.Watch(inter_ch, func(worker, subj, data string) bool {
		total += 1
		if comm.Timestamp()-record_time > int64(time.Second) {
			t.Log(">>>>>>>>>>>>>> speed", total-recrd, "/s")
			recrd = total
			record_time = comm.Timestamp()
		}
		return true
	})
	t.Log("send msg")
	for {
		msg := RandStr(rand.Intn(8))
		message.Put(start_ch, msg)
	}
}

package test

import (
	"cloudflow/sdk/golang/cloudflow/chops"
	"cloudflow/sdk/golang/cloudflow/comm"
	"sync"
	"testing"
	"time"
)

func Test_ch_correct(t *testing.T) {
	// config
	map_ins := 10
	test_msg_count := 10_0000

	wg := sync.WaitGroup{}
	map_chs := []string{}
	for i := 0; i < map_ins; i++ {
		map_chs = append(map_chs, comm.Itos(i))
	}
	imp := "nats"
	cfg := comm.CFG{
		"app_id": "app_id",
		"host":   "127.0.0.1",
		"port":   4222,
	}
	message := chops.GetChOpsImp(imp, cfg)
	output_ch := []string{"out"}
	sum := 0.0
	target := 0
	input_ch := []string{"int"}
	wg.Add(1)
	w_time := comm.Timestamp()
	r_count := 0
	rec_count := 0
	message.Watch("out", output_ch, func(worker, subj, data string) bool {
		if data == "EXIT" {
			t.Log("recive exit, exit!")
			wg.Done()
		} else {
			sum += comm.FrJson(data).(float64)
			rec_count += 1
		}
		now_time := comm.Timestamp()
		if now_time-w_time > int64(time.Second) {
			t.Log(">>> ouput speed", rec_count-r_count, int(sum), "->", target)
			r_count = rec_count
			w_time = now_time
		}
		return true
	})
	message.Watch("map", map_chs, func(worker, subj, data string) bool {
		message.Put(output_ch, data)
		return true
	})
	for idx := range map_chs {
		message.Watch("dyn+"+comm.Itos(idx), input_ch, func(worker, subj, data string) bool {
			message.Put(map_chs[idx:idx+1], data)
			return true
		})
	}

	// simulate dynamic Node add
	add_new_consumer := func() {
		t.Log("add new consumer")
		for i := 0; i < map_ins; i++ {
			message.Watch(comm.Itos(i), input_ch, func(worker, subj, data string) bool {
				message.Put(output_ch, data)
				return true
			})
		}
	}

	s_time := comm.Timestamp()
	s_count := 0
	add_new_csm := false
	for i := 0; i < test_msg_count; i++ {
		message.Put(input_ch, comm.AsJson(i))
		target += i
		n_time := comm.Timestamp()
		if n_time-s_time > int64(time.Second) {
			percent := 100 * float64(i) / float64(test_msg_count)
			t.Log(">>> input speed", i-s_count, "percent:", comm.FmStr("%2.f %%", percent))
			s_count = i
			s_time = n_time
			if !add_new_csm && percent > 50 {
				add_new_consumer()
				add_new_csm = true
			}
		}
	}
	message.Put(input_ch, "EXIT")
	t.Log("wait ...")
	wg.Wait()
	t.Log("target:", target, "result:", int(sum))
	comm.Assert(int(sum) == target, "check result fail, delta: %d", int(sum)-target)
}

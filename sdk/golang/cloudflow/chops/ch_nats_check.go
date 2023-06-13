package chops

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"strings"
)

func CheckNats(host string, port int) bool {

	test_success := false
	test_ch := make(chan int)
	test_channels := []string{"A", "B", "C"}
	test_stream := "testcf-" + cf.AsMd5(cf.TimestampStr()+"TEST_JetStream")
	test_msg_size := 10
	test_msg_idx := 10

	conurl := host
	if !strings.Contains(host, "/") {
		conurl = "nats://" + host + ":" + cf.Itos(port)
	}
	nats := NewNatsChOp(conurl, test_stream)
	wkey1 := nats.Watch(test_channels, func(wkid, ch string, a string) bool {
		test_msg_idx += 1
		if test_msg_idx == test_msg_size*len(test_channels) {
			test_success = true
			close(test_ch)
		}
		return true
	})

	wkey2 := nats.Watch(test_channels, func(wkid, ch string, a string) bool {
		test_msg_idx += 1
		if test_msg_idx == test_msg_size*len(test_channels) {
			test_success = true
			close(test_ch)
		}
		return true
	})

	for i := 0; i < test_msg_size; i++ {
		cf.Assert(nats.Put(test_channels, "Data"+cf.Itos(i)), "Put nats fail")
	}
	<-test_ch
	nats.CStop(wkey1)
	nats.CStop(wkey2)
	nats.Close()
	return test_success
}

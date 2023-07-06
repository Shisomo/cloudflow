package examples

import (
	"cloudflow/sdk/golang/cloudflow/comm"
	"math/rand"
	"time"
)

var txtbook = "qwertyuiopasdfghjklzxcvbnm0987654321"

func randWord() string {
	length := 1 + rand.Intn(10)
	bytes := []byte(txtbook)
	result := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

func mutex_word(count int) []string {
	ret := make([]string, len(txtbook))

	return ret
}

func Main_GigaSort(args ...string) {
	// Need file storage support
	comm.Log("TBD")
}

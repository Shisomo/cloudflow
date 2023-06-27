package main

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/comm"
	"math/rand"
	"strings"
	"time"
)

func statistics(app *cf.App) string {
	return "Hello word"
}

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

func ReadWords(self *cf.Node, count int) string {
	if self.Exited() {
		return ""
	}
	// init
	if self.UserData == nil {
		self.MsgLog("try random gen data with size: ", count)
		self.UserData = count
	}
	// exit
	if self.UserData.(int) <= 0 {
		self.MsgLog("read data complete: ", count-self.UserData.(int), " sended!")
		self.Exit("Norm")
		return ""
	}
	// Gen txt
	words := []string{}
	words_size := rand.Intn(100)
	for i := 0; i < words_size; i++ {
		words = append(words, RandStr(rand.Intn(10)+1))
	}
	remain_count := self.UserData.(int) - words_size
	self.UserData = remain_count

	if remain_count%10_0000 == 1 {
		comm.Log("remain words:", remain_count, "read speed:", int(self.CallSpeed(true)))
	}
	return strings.Join(words, " ")
}

func CountWords(self *cf.Node, txt string) map[string]float64 {
	ret := map[string]float64{}
	processed := 0
	for _, word := range strings.Split(txt, " ") {
		if word == "" {
			continue
		}
		count := 1.0
		processed += 1
		c, h := ret[word]
		if h {
			count = count + 1
		} else {
			count = count + c
		}
		ret[word] = count
	}
	if self.CallCount()%1000 == 0 {
		comm.Log("coun speed:", int(self.CallSpeed(true)))
	}
	if self.UserData == nil {
		self.UserData = processed
	} else {
		self.UserData = processed + self.UserData.(int)
	}
	if self.Exited() {
		self.MsgLogf("processed %d words", self.UserData.(int))
	}
	return ret
}

func ReduceWords(se *cf.Node, statistic []map[string]float64) {
	if se.UserData == nil {
		se.UserData = map[string]int{}
	}
	// merge
	ret := se.UserData.(map[string]int)
	for _, data := range statistic {
		if data == nil {
			continue
		}
		for k, c := range data {
			if k == "" {
				continue
			}
			count, has := ret[k]
			if has {
				ret[k] = count + int(c)
			} else {
				ret[k] = int(c)
			}
		}
	}
	// log
	all_count := 0
	for _, v := range ret {
		all_count += v
	}
	if se.CallCount()%1000 == 0 {
		comm.Log("words:", len(ret), "all words:", all_count, "redu speed:", int(se.CallSpeed(true)))
	}
	if se.Exited() {
		se.MsgLogf("all words: %d, redu speed:%d", all_count, int(se.CallSpeed(true)))
	}
}

func main() {
	comm.LogSetPrefix("test-word-count ")
	comm.Log("Version", comm.Version())
	var app = cf.NewApp("test-app")
	var ses = app.CreateSession("session-1")
	var flw = ses.CreateFlow("flow-1")
	app.Reg(statistics, "record the process")
	flw.Add(ReadWords, "read", 100_0000).Map(CountWords, "count", 10).Reduce(ReduceWords, "reduce", 5)
	app.Run()
}

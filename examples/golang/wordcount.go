package main

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/comm"
	"math/rand"
	"strings"
)

func statistics(app *cf.App) string {
	return "Hello word"
}

func ReadWords(self *cf.Node, count int) string {
	// init
	if self.UserData == nil {
		self.MsgLog("try random gen data with size: ", count)
		self.UserData = count
	}
	// exit
	if self.UserData.(int) <= 0 {
		self.MsgLog("read data complete: ", count-self.UserData.(int), " sended!", " calls: ", self.CallCount()-1)
		self.Exit("Norm")
		return ""
	}
	// Gen txt
	words := []string{}
	remain_count := self.UserData.(int)
	var words_size int
	if remain_count < 200 {
		words_size = remain_count
	} else {
		words_size = rand.Intn(100) + 1
	}

	for i := 0; i < words_size; i++ {
		words = append(words, comm.RandStr(rand.Intn(10)+1))
	}
	remain_count -= words_size
	self.UserData = remain_count

	if remain_count%10_0000 == 1 {
		comm.Log("remain words:", remain_count, "read speed:", int(self.CallSpeed(true)))
	}
	comm.Assert(len(words) > 0, "find empty txt")
	return strings.Join(words, " ")
}

func CountWords(self *cf.Node, txt string) map[string]float64 {
	ret := map[string]float64{}
	// ignore empty value
	if txt == "" {
		return ret
	}
	for _, word := range strings.Split(txt, " ") {
		comm.Assert(word != "", "find empty key in: %s", txt)
		count, has := ret[word]
		if has {
			count = count + 1
		} else {
			count = 1
		}
		ret[word] = count
	}
	return ret
}

func ReduceWords(se *cf.Node, statistic []map[string]float64) map[string]float64 {
	if se.UserData == nil {
		se.UserData = map[string]float64{}
	}
	// merge
	ret := se.UserData.(map[string]float64)
	for _, data := range statistic {
		if data == nil {
			continue
		}
		for k, c := range data {
			comm.Assert(k != "", "key is empty")
			count, has := ret[k]
			if has {
				ret[k] = count + c
			} else {
				ret[k] = c
			}
		}
	}
	// log
	all_count := 0.0
	for _, v := range ret {
		all_count += v
	}
	if se.CallCount()%1000 == 0 {
		comm.Log("words:", len(ret), "all words:", int(all_count), "redu speed:", int(se.CallSpeed(true)))
	}
	if se.Exited() {
		se.MsgLogf("all words: %d, redu speed:%d, calls: %d", int(all_count), int(se.CallSpeed(true)), se.CallCount())
	}
	return ret
}

func main() {
	comm.LogSetPrefix("test-word-count ")
	comm.Log("Version", comm.Version())
	var app = cf.NewApp("test-app")
	var ses = app.CreateSession("session-1")
	var flw = ses.CreateFlow("flow-1")
	app.Reg(statistics, "record the process")
	flw.Add(ReadWords, "read", 1000_0000).Map(CountWords, "count", 10).Reduce(ReduceWords, "reduce", 10)
	app.Run()
}

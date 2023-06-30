package main

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/comm"
	"math/rand"
	"os"
	"strings"
	"time"
)

func RandStr(length int) string {
	str := "0123456789"
	bytes := []byte(str)
	result := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
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
		words = append(words, RandStr(rand.Intn(3)+1))
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

func ReduceWords(se *cf.Node, statistic []map[string]float64, is_final bool) map[string]float64 {
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
	if se.Exited() {
		se.MsgLogf("%d =>words: %d, redu speed:%d, calls: %d", len(ret), int(all_count), int(se.CallSpeed(true)), se.CallCount())
	}
	if !is_final {
		if !se.Exited() {
			se.IgnoreRet()
		}
	}
	return ret
}

func main() {
	comm.LogSetPrefix("test-word-count ")
	comm.Log("Version", comm.Version())
	var app = cf.NewApp("test-app")
	var ses = app.CreateSession("session-1")
	var flw = ses.CreateFlow("flow-1")
	if len(os.Args) > 1 {
		if os.Args[1] == "two" {
			// DAG:
			//                   /count1         /reduce1
			//   read1 \         |count2         |reduce2
			//     ...  \ --->   |...      ----> |...      -----> all
			//   read10 /         \count20        \reduce20
			//
			flw.Add(ReadWords, "read", 1_000_000, cf.OpInsCount(10)).Map(
				CountWords, "count", 20).Reduce(
				ReduceWords, "reduce", 20, false, cf.OpInsCount(20)).Reduce(
				ReduceWords, "all", 2, true, cf.OpPerfLogInter(1))
			app.Run()
			return
		} else {
			comm.Log("option ", os.Args[1], " not supported")
		}
	}
	comm.Log("use simple flow")
	// DAG:
	//                / count1
	//      read ---->  ...     ---> reduce
	//                \ count10
	flw.Add(ReadWords, "read", 10_000_000).Map(CountWords, "count", 10).Reduce(ReduceWords, "reduce", 10, true)
	app.Run()
}

package examples

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/comm"
	"math/rand"
	"sort"
	"strings"
	"time"
)

var txtbook = "qwertyuiopasdfghjklzxcvbnm0987654321"

// 测试数据生成
// Function randWord 随机以txtbook中字符组成1-10位字符串string
func rand_word() string {
	length := 1 + rand.Intn(10)
	bytes := []byte(txtbook)
	result := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

// 读数据
// Add(readData, "read", 100_000, cf.OpInsCount(2))
// Function readData
func read_data(self *cf.Node, count int) string {
	if self.UserData == nil {
		comm.Info("start read ", count, " words")
		self.UserData = count
	}
	word_to_gen := self.UserData.(int)
	if word_to_gen <= 0 {
		comm.Info("read ", count, " words complete")
		self.IgnoreRet()
		self.Exit("gen data complete")
	}
	self.UserData = word_to_gen - 1
	w := rand_word()
	comm.Assert(w != "", "gen word fail")

	return w
}

// Add(dispatch, "dispath", 0, cf.OpOutType(comm.NODE_OUYPE_MUT), cf.OpDispatchSize(len(txtbook)), cf.OpInsCount(2))
// Function dispatch 负责大文件分片分配调度
func dispatch(self *cf.Node, word string, index int) []string {
	ret := make([]string, len(txtbook))
	if self.Exited() {
		// return ret
		return ret
	}
	comm.Assert(self.OuType == comm.NODE_OUYPE_MUT, "")
	comm.Assert(word != "", "find empty input")
	ws := len(word)
	comm.Assert(index < ws, "index need small then word size")
	idx := strings.Index(txtbook, word[index:index+1])
	comm.Assert(idx >= 0, "cannot find prefix: %s in: %s", txtbook, word[index:index+1])
	self.MarkRet(idx)
	ret[idx] = word
	return ret
}

// Dispatch(insertSort, "sort", len(txtbook)
// Function insert_sort
func insert_sort(self *cf.Node, w string, allprefix string) string {
	prefix := string(allprefix[self.SubIdx])
	if self.UserData == nil {
		self.UserData = []string{}
	}
	sorted_list := self.UserData.([]string)
	if !self.Exited() {
		if w != "" {
			self.UserData = append(sorted_list, w)
		}
		return ""
	} else {
		st := sort.StringSlice(sorted_list)
		st.Sort()
		comm.Info("exit prefix: ", prefix, " self.prefix", self.SubIdx, " size:", st.Len())

		//------------------------------------------------------------------
		// 存储排序好的StringSlice
		// 首先实例化本任务节点所属session的storage
		storage := self.Flow.Sess.GetStorageOps()
		// 打开文件
		buf := storage.FileOpen(prefix)
		// 写入文件
		for _, v := range st {
			buf.WriteString(v + " ")
		}
		// 上传文件
		storage.FileWrite(prefix, buf)
		storage.FileClose(prefix)
		return prefix
	}
}

// // Function merge_sort read and merge the sorted data from shared storage
func merge_sort(self *cf.Node, prefix string) {
	// 实例化本任务节点所属session的storage
	storage := self.Flow.Sess.GetStorageOps()
	// 添加上一届点所有分片信息至UserData
	if prefix != "" {
		if self.UserData == nil {
			self.UserData = []string{}
		}
		self.UserData = append(self.UserData.([]string), prefix)
	}
	// merge操作，通过对分片的标签的排序实现
	if self.Exited() {

		keys := self.UserData.([]string)
		st := sort.StringSlice(keys)
		st.Sort()
		// merge结果文件创建
		fileMergeBuffer := storage.FileOpen(storage.Scope())
		// merge结果汇总
		for _, v := range st {
			sortBuffer := storage.FileOpen(v)
			fileMergeBuffer.ReadFrom(&sortBuffer)
			storage.FileWrite(storage.Scope(), fileMergeBuffer)
			fileMergeBuffer.Reset() // buf是值传递，在storage中reset没用
			storage.FileClose(v)
		}
		// merge结果上传至共享存储
		storage.FileClose(storage.Scope())
	}
}

func Main_GigaSort(args ...string) {
	// Need file storage support
	app := cf.NewApp("gigasort")
	// 新建session，并以session的名"gigasort-Session-1"创建共享存储，实现不同session之间的隔离
	flw := app.CreateSession("gigasort-Session-1").CreateFlow("flow1")
	//                      -> sort(a,..) \
	//                     /-> sort(b,..)  |
	//     read -> dispatch -> sort(c,..)  |->merge
	//                     \-> ...         |
	//                      -> sort(z,..) /
	//

	// 首先添加文件生成的100_000個通过add readdata
	// flw.Add(read_data, "read", 8060000, cf.OpInsCount(2)).Add(dispatch, "dispath", 0,
	// 	cf.OpOutType(comm.NODE_OUYPE_MUT), cf.OpDispatchSize(len(txtbook)), cf.OpInsCount(2)).Dispatch(insert_sort,
	// 	"sort", len(txtbook), txtbook).Add(merge_sort, "merge")
	// test
	flw.Add(read_data, "read", 806000, cf.OpInsCount(1)).Add(dispatch, "dispath", 0,
		cf.OpOutType(comm.NODE_OUYPE_MUT), cf.OpDispatchSize(len(txtbook)), cf.OpInsCount(1)).Dispatch(insert_sort,
		"sort", len(txtbook), txtbook).Add(merge_sort, "merge")
	app.Run()
}

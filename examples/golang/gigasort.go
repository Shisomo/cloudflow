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

func dispatch(self *cf.Node, word string, index int) []string {
	ret := make([]string, len(txtbook))
	if self.Exited() {
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

// TBD: save data to shared storage
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
		comm.Info("exit prefix: ", prefix, " size:", st.Len())
		return prefix
	}
}

// TBD: read and merge the sorted data from shared storage
func merge_sort(self *cf.Node, prefix string) {
	if prefix != "" {
		if self.UserData == nil {
			self.UserData = []string{}
		}
		self.UserData = append(self.UserData.([]string), prefix)
	}
	if self.Exited() {
		keys := self.UserData.([]string)
		st := sort.StringSlice(keys)
		st.Sort()
		comm.Info("exit merge: ", st)
		comm.Assert(st.Len() == len(txtbook), "merge key need == txtbook")
	}
}

func Main_GigaSort(args ...string) {
	// Need file storage support
	app := cf.NewApp("gigasort")
	flw := app.CreateSession("default").CreateFlow("flow1")
	//                      -> sort(a,..) \
	//                     /-> sort(b,..)  |
	//     read -> dispatch -> sort(c,..)  |->merge
	//                     \-> ...         |
	//                      -> sort(z,..) /
	flw.Add(read_data, "read", 100_000, cf.OpInsCount(2)).Add(dispatch, "dispath", 0,
		cf.OpOutType(comm.NODE_OUYPE_MUT), cf.OpDispatchSize(len(txtbook)), cf.OpInsCount(2)).Dispatch(insert_sort,
		"sort", len(txtbook), txtbook).Add(merge_sort, "merge")
	app.Run()
}

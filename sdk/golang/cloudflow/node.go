package cloudflow

import (
	"cloudflow/sdk/golang/cloudflow/chops"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/fileops"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"cloudflow/sdk/golang/cloudflow/task"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"
)

type Node struct {
	// basic-params
	Name     string        `json:"name"`
	Func     interface{}   `json:"-"`
	Flow     *Flow         `json:"-"`
	Uuid     string        `json:"uuid"`
	Idx      int           `json:"index"`
	SubIdx   int           `json:"subidx"`
	DispSize int           `json:"dispatchsize"`
	PreNodes []*Node       `json:"-"`
	NexNodes []*Node       `json:"-"`
	ExArgs   []interface{} `json:"-"`
	Batch    int           `json:"batch"`
	InsCount int           `json:"inscount"`
	InsRange []int         `json:"insrange"`
	InType   string        `json:"intype"` // Queue [default], Sub
	OuType   string        `json:"outype"` // Single [default], Mut, All
	InChan   []int         `json:"inchan"` // Empty [default] all pre-nodes output, pre-node's output index
	// extra-params
	outCount  int             `json:"-"`
	inCount   int             `json:"-"`
	UserData  interface{}     `json:"-"`
	startTime int64           `json:"-"`
	callCount int64           `json:"-"`
	recTime   int64           `json:"-"`
	recCount  int64           `json:"-"`
	PerfInter int             `json:"-"`
	kvOps     kvops.KVOp      `json:"-"`
	chOps     chops.ChannelOp `json:"-"`
	fileOps   fileops.FileOps `json:"-"`
	defaultv  []interface{}   `json:"-"`
	ignoreRet bool            `json:"-"`
	retMask   map[int]int     `json:"-"`
	cf.CommStat
}

func (node *Node) MarshalJSON() ([]byte, error) {
	type JNode Node
	func_name := strings.Replace(reflect.ValueOf(node.Func).String(), "func(",
		runtime.FuncForPC(reflect.ValueOf(node.Func).Pointer()).Name()+"(", 1)
	return json.Marshal(&struct {
		*JNode
		Func string `json:"func"`
	}{
		JNode: (*JNode)(node),
		Func:  func_name,
	})
}

func (node *Node) String() string {
	return fmt.Sprintf("Node(%s, %s)", node.Uuid, node.Name)
}

var __node_index__ int = 0

func newNode(flow *Flow, kw ...map[string]interface{}) *Node {
	var node = Node{
		Idx:       __node_index__,
		Flow:      flow,
		UserData:  nil,
		kvOps:     nil,
		chOps:     nil,
		fileOps:   nil,
		Batch:     1,
		PerfInter: 0,
		InType:    cf.NODE_ITYPE_QUEUE,
		OuType:    cf.NODE_OUYPE_SGL,
		InsRange:  []int{},
		InChan:    []int{},
		retMask:   map[int]int{},
	}
	node.CTime = cf.Timestamp()
	__node_index__ += 1
	node.Update(kw...)
	flow.AddNode(&node)
	node.Parent = cf.DotS(cf.K_AB_FLOW, flow.Uuid)
	node.AppUid = flow.Sess.App.Uuid
	node.Cstat = cf.K_STAT_WAIT
	node.IsExit = false
	node.defaultv = cf.FuncEmptyRet(node.Func) // FIXME: need custom
	cf.Assert(node.defaultv != nil, "default return value is nil")
	return &node
}

func (node *Node) Update(kw ...map[string]interface{}) {
	cf.UpdateObject(node, kw...)
	identies := cf.DotS(node.Flow.Uuid, cf.Itos(node.Idx), node.Name, cf.Itos(node.SubIdx))
	node.Uuid = cf.AsMd5(identies)
}

func MakeNode(flow *Flow, fc interface{}, name string, ex_args ...interface{}) *Node {
	var new_node = newNode(flow, map[string]interface{}{
		"Name":     name,
		"Func":     fc,
		"ExArgs":   ex_args,
		"InsCount": 1,
	})
	ref_fc := reflect.ValueOf(fc).Type()
	new_node.inCount = ref_fc.NumIn()
	new_node.outCount = ref_fc.NumOut()
	if new_node.OuType == cf.NODE_OUYPE_MUT {
		cf.Assert(new_node.outCount == 1, "Need only one return data, but %d find", new_node.outCount)
		cf.Assert(ref_fc.Out(0).Kind() == reflect.Slice, "Need List return")
	}
	return new_node
}

func (node *Node) append(fc interface{}, name string, args ...interface{}) *Node {
	// 分析参数 将CloudFlowOption类型参数与其他参数分开
	ex_args, options := ParsOptions(args)
	new_node := MakeNode(node.Flow, fc, name, ex_args...)
	new_node.Update(options)
	if len(new_node.InChan) > 0 {
		cf.Assert(node.OuType == cf.NODE_OUYPE_MUT,
			"node.OuType can not be %s", node.OuType)
	}
	// append
	new_node.PreNodes = append(new_node.PreNodes, node)
	node.NexNodes = append(node.NexNodes, new_node)
	return new_node
}

func (node *Node) Add(fc interface{}, name string, args ...interface{}) *Node {
	return node.append(fc, name, args...)
}

func (node *Node) Map(fc interface{}, name string, count int, args ...interface{}) *Node {
	var new_node = node.append(fc, name, args...)
	new_node.InsCount = count
	return new_node
}

func (node *Node) Reduce(fc interface{}, name string, batch int, args ...interface{}) *Node {
	v := node.append(fc, name, args...)
	v.Batch = batch
	return v
}

func (node *Node) Dispatch(fc interface{}, name string, dispatch_size int, arg ...interface{}) *Node {
	cf.Assert(node.OuType == cf.NODE_OUYPE_MUT, "only mut output support dispatch")
	n := node.append(fc, name, arg...)
	n.InsCount = dispatch_size
	n.InChan = cf.Range(0, dispatch_size)
	n.InType = cf.NODE_ITYPE_INSPC
	return n
}

func Merge(fc interface{}, name string, nodes []*Node, args ...interface{}) *Node {
	ex_args, options := ParsOptions(args)
	cf.Assert(len(nodes) > 1, "Merge need Nodes(%d) > 1", len(nodes))
	node := MakeNode(nodes[1].Flow, fc, name, ex_args...)
	node.Update(options)
	chsize := len(node.InChan)
	cf.Assert(chsize == 0 || chsize == len(nodes), "InChan need be empty size == nodes.size (%d != %d)", chsize, len(nodes))
	var flow *Flow
	for idx, n := range nodes {
		if flow == nil {
			flow = n.Flow
		}
		if n.OuType == cf.NODE_OUYPE_MUT {
			cf.Assert(chsize == len(nodes), "need assigne InChan option")
		}
		cf.Assert(flow == n.Flow, "Only can merge nodes in the same flow (%s != %s[%d])", flow.Name, n.Flow.Name, idx)
		if chsize > 0 {
			sub_chsize := node.InChan[idx]
			cf.Assert(sub_chsize < n.outCount, "check inchan fail")
		}
		// append
		n.NexNodes = append(n.NexNodes, node)
		node.PreNodes = append(node.PreNodes, n)
	}
	return node
}

func (node *Node) GetSession() *Session {
	return node.Flow.Sess
}

func (node *Node) TimeFromStart() int64 {
	return cf.Timestamp() - node.startTime
}

func (node *Node) TimeFromStartSecond() int64 {
	return node.TimeFromStart() / int64(time.Second)
}

func (node *Node) CallSpeed(reset bool) float64 {
	now_time := cf.Timestamp()
	deta := (now_time - node.recTime)
	speed := float64(time.Second) * float64(node.callCount-node.recCount) / float64(deta)
	if reset {
		node.recCount = node.callCount
		node.recTime = now_time
	}
	return speed
}

func (node *Node) CallCount() int64 {
	return node.callCount
}

// RunInterface
func (node *Node) Exited() bool {
	return node.IsExit
}

func (node *Node) StartCall() {
	node.callCount = 0
	node.startTime = cf.Timestamp()
	node.recCount = 0
	node.recTime = cf.Timestamp()
}

func (node *Node) PreCall() {
	node.callCount += 1
	node.ignoreRet = false
	node.retMask = map[int]int{}
}

func (node *Node) getDefautRet(idx ...int) []interface{} {
	if node.OuType != cf.NODE_OUYPE_MUT {
		return node.defaultv
	}
	cf.Assert(len(node.defaultv) == 1, "MUT out, need only one list return")
	ret := []interface{}{
		cf.AsSliceValue(node.defaultv[0]),
	}
	return ret
}

func (node *Node) Call(a ...interface{}) []interface{} {
	args := []interface{}{node}
	args = append(args, a...)
	args = append(args, node.ExArgs...)
	node.PreCall()
	ret := cf.FuncCall(node.Func, args)
	if node.Exited() {
		node.callCount -= 1
	}
	switch node.OuType {
	case cf.NODE_OUYPE_SGL:
		return ret
	case cf.NODE_OUYPE_MUT:
		cf.Assert(len(ret) == 1, "need only one List return, but find: %d", len(ret))
		return cf.JAsType(ret[0], reflect.ValueOf([]interface{}{}).Type()).([]interface{})
	default:
		cf.Assert(false, "node.outype(%s) not support", node.OuType)
	}
	return ret
}

func (node *Node) SyncState() {
	for key, value := range cf.AsKV(node) {
		rkey := cf.DotS(cf.K_AB_NODE, node.Uuid, key)
		node.kvOps.Set(rkey, value)
	}
}

func (node *Node) Exit(reason string) {
	node.IsExit = true
	node.ExitLog = reason
}

func (node *Node) InOutChs() ([]string, []string) {
	// Need only one input channel from each pre-node
	// Output
	ou_ch := []string{}
	ch_id := strings.Split(node.Uuid, "-")[0]
	switch node.OuType {
	case cf.NODE_OUYPE_MUT:
		for i := 0; i < node.DispSize; i++ {
			ou_ch = append(ou_ch, cf.DotS(ch_id, "out", cf.Astr(i)))
		}
	case cf.NODE_OUYPE_SGL:
		ou_ch = []string{cf.DotS(ch_id, "out")}
	default:
		cf.Assert(false, "node out type(%s) not support", node.OuType)
	}
	// Input
	in_ch := []string{}
	for idx, n := range node.PreNodes {
		pn_id := strings.Split(n.Uuid, "-")[0]
		switch n.OuType {
		case cf.NODE_OUYPE_MUT:
			if node.InType == cf.NODE_ITYPE_INSPC {
				in_ch = append(in_ch, cf.DotS(n.Uuid, "out", cf.Astr(node.SubIdx)))
			} else {
				nid := node.InChan[idx]
				cf.Assert(nid > 0 && nid < n.outCount, "channel index error: 0 < %d < %d", nid, n.outCount)
				in_ch = append(in_ch, cf.DotS(n.Uuid, "out", cf.Astr(nid)))
			}
		case cf.NODE_OUYPE_SGL:
			in_ch = append(in_ch, cf.DotS(pn_id, "out"))
		default:
			cf.Assert(false, "node out type(%s) not support", n.OuType)
		}
	}
	// uuid-0 => uuid
	return in_ch, ou_ch
}

func (node *Node) SetSubIdx(idx int) {
	node.SubIdx = idx
}

func (node *Node) SetKVOps(ops kvops.KVOp) {
	node.kvOps = ops
}

func (node *Node) FuncName() string {
	return cf.FuncName(node.Func)
}

func (node *Node) InstanceCount() int {
	return node.InsCount
}

func (node *Node) GetBatchSize() int {
	return node.Batch
}

func (node *Node) GetExitChs(ichs []string) (map[string][]interface{}, bool) {
	ch_val := map[string][]interface{}{}
	all_exit := true
	for _, n := range node.PreNodes {
		is_exit := node.kvOps.Get(cf.DotS(cf.K_AB_NODE, n.Uuid, cf.K_MEMBER_IS_EXIT))
		if is_exit == nil {
			all_exit = false
			continue
		}
		if !is_exit.(bool) {
			all_exit = false
		} else {
			// mark exit
			for _, ch := range ichs {
				if strings.Contains(ch, n.Uuid) {
					ch_val[ch] = n.getDefautRet(node.SubIdx)
				}
			}
		}
		ins_count := int(node.kvOps.Get(cf.DotS(cf.K_AB_NODE, n.Uuid, cf.K_MEMBER_INSCOUNT)).(float64))
		for i := 1; i < ins_count; i++ {
			uuid := n.Uuid + "-" + cf.Itos(i)
			is_exit := node.kvOps.Get(cf.DotS(cf.K_AB_NODE, uuid, cf.K_MEMBER_IS_EXIT))
			if is_exit == nil {
				all_exit = false
				continue
			}
			if !is_exit.(bool) {
				all_exit = false
			} else {
				// mark exit
				for _, ch := range ichs {
					if strings.Contains(ch, n.Uuid) {
						ch_val[ch] = n.getDefautRet(node.SubIdx)
					}
				}
			}
		}
	}
	return ch_val, all_exit
}

func (node *Node) UpdateUUID(node_key string) {
	node.Uuid = strings.Replace(node_key, cf.K_AB_NODE+".", "", 1)
}

func (node *Node) SetMsgOps(ops chops.ChannelOp) {
	node.chOps = ops
}

func (node *Node) msg(txt string) {
	cf.Log(txt)
	node.chOps.Put([]string{node.AppUid + ".log"}, txt)
}

func (node *Node) MsgLog(a ...interface{}) {
	node.msg(fmt.Sprint(a...))
}

func (node *Node) MsgLogf(fmt string, a ...interface{}) {
	node.msg(cf.FmStr(fmt, a...))
}

func (node *Node) UUID() string {
	return node.Uuid
}

func (node *Node) IsIgnoreRet() bool {
	if node.ignoreRet {
		return true
	}
	if len(node.NexNodes) > 0 {
		return false
	}
	return true
}

func (node *Node) IgnoreRet() {
	node.ignoreRet = true
}

func (node *Node) AsTask() task.Task {
	return task.Task{
		List_key: cf.DotS(node.Parent, cf.K_AB_NODE),
		Uuid_key: cf.DotS(cf.K_AB_NODE, node.Uuid),
	}
}

func (node *Node) PerfLogInter() int {
	return node.PerfInter
}

func (node *Node) GetName() string {
	return node.Name
}

func (node *Node) MarkRet(msk ...int) {
	for _, v := range msk {
		node.retMask[v] = v
	}
}

func (self *Node) makeResponse(msg_index int64, outch []string, rets []interface{}) {
	cf.Assert(len(rets) > 0, "need ret data > 0, ret: %s", rets)
	if self.OuType == cf.NODE_OUYPE_SGL {
		self.chOps.Put(outch, cf.MakeMsg(msg_index, rets, cf.K_MESSAGE_NORM))
	} else {
		cf.Assert(self.OuType == cf.NODE_OUYPE_MUT, "need OuType be: %s", cf.NODE_OUYPE_MUT)
		cf.Assert(len(outch) == self.DispSize,
			"out channels not match in Mut Output mode (out:%d != dispatchsize:%d), ch: %s", len(outch), self.DispSize, outch)
		for i := 0; i < self.DispSize; i++ {
			if len(self.retMask) > 0 {
				_, has := self.retMask[i]
				if !has {
					continue
				}
			}
			self.chOps.Put(outch[i:i+1], cf.MakeMsg(msg_index, rets[i:i+1], cf.K_MESSAGE_NORM))
		}
	}
}

func (node *Node) SetFileOps(ops fileops.FileOps) {
	node.fileOps = ops
}
func (node *Node) GetFileOps() fileops.FileOps {
	return node.fileOps
}

func (self *Node) Run() int64 {
	chs_i, chs_o := self.InOutChs()
	cf.Log("start worker(", self.Uuid, ") with:", chs_i, "=>", chs_o, self.FuncName())

	msg_index := int64(0)
	self.StartCall()
	time_ch_exit := cf.Timestamp()
	self.Cstat = cf.K_STAT_WORK
	self.SyncState()

	if len(chs_i) < 1 {
		// data source node
		for {
			rets := self.Call()
			if !self.IsIgnoreRet() {
				self.makeResponse(msg_index, chs_o, rets)
			}
			if self.Exited() {
				break
			}
			msg_index += 1
		}
	} else {
		// data process
		perf_log_inter := self.PerfLogInter()
		exit_loop := false
		cf.Log("watch:", chs_i)
		data_cache := InitChDataCache(chs_i, self.GetBatchSize(), perf_log_inter > 0)
		cnkeys := []string{}
		switch self.InType {
		case cf.NODE_ITYPE_QUEUE, cf.NODE_ITYPE_INSPC:
			cnkeys = append(cnkeys, self.chOps.Watch(self.UUID(), chs_i, func(worker, subj, data string) bool {
				cf.Assert(!exit_loop, "get queue data from empty node: %s", data)
				data_cache.Put(subj, data)
				return true
			})...)
		case cf.NODE_ITYPE_SUBSC:
			cnkeys = append(cnkeys, self.chOps.Sub(self.UUID(), chs_i, func(worker, subj, data string) bool {
				cf.Assert(!exit_loop, "get sub data from empty node: %s", data)
				data_cache.Put(subj, data)
				return true
			})...)
		default:
			cf.Assert(false, "node intype:%s not supported", self.InType)
		}
		// loop check and callback
		loop_count := 0
		for {
			loop_count += 1
			args_get, all_dfv := data_cache.Get()
			if len(args_get) < 1 || all_dfv {
				if cf.Timestamp()-time_ch_exit > int64(time.Second) {
					// check exit
					ch_val, all_exit := self.GetExitChs(chs_i)
					if all_exit && all_dfv {
						if self.chOps.CEmpty(cnkeys) {
							self.chOps.CStop(cnkeys)
							self.Exit("no input")
							exit_loop = true
						}
					}
					data_cache.SetExitValue(cf.KVMakeMsg(ch_val))
					time_ch_exit = cf.Timestamp()
				}
				time.Sleep(100 * time.Millisecond)
				if !exit_loop {
					continue
				}
			}
			time_ch_exit = cf.Timestamp()
			rets := self.Call(args_get...)
			data_cache.UpdateExSpeed("call", time_ch_exit)
			if !self.IsIgnoreRet() {
				self.makeResponse(msg_index, chs_o, rets)
				msg_index += 1
			}
			// log performance statistics
			if perf_log_inter > 0 && loop_count%perf_log_inter == 0 {
				cf.Log(data_cache.Stat())
				data_cache.ClearStat()
			}
			if exit_loop {
				break
			}
		}
	}
	return msg_index
}

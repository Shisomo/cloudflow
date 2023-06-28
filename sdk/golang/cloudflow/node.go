package cloudflow

import (
	"cloudflow/sdk/golang/cloudflow/chops"
	cf "cloudflow/sdk/golang/cloudflow/comm"
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
	Name      string          `json:"name"`
	Func      interface{}     `json:"-"`
	Flow      *Flow           `json:"-"`
	Uuid      string          `json:"uuid"`
	Idx       int             `json:"index"`
	SubIdx    int             `json:"subidx"`
	PreNodes  []*Node         `json:"-"`
	NexNodes  []*Node         `json:"-"`
	ExArgs    []interface{}   `json:"-"`
	Batch     int             `json:"batch"`
	InsCount  int             `json:"inscount"`
	CTime     int64           `json:"ctime"`
	UserData  interface{}     `json:"-"`
	startTime int64           `json:"-"`
	callCount int64           `json:"-"`
	recTime   int64           `json:"-"`
	recCount  int64           `json:"-"`
	kvOps     kvops.KVOp      `json:"-"`
	chOps     chops.ChannelOp `json:"-"`
	defaultv  []interface{}   `json:"-"`
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

func NewNode(flow *Flow, kw ...map[string]interface{}) *Node {
	var node = Node{
		Idx:      __node_index__,
		Flow:     flow,
		CTime:    cf.Timestamp(),
		UserData: nil,
		kvOps:    nil,
		chOps:    nil,
		Batch:    1,
	}
	__node_index__ += 1
	node.Update(kw...)
	node.Parent = cf.DotS(cf.K_AB_FLOW, flow.Uuid)
	node.AppUid = flow.Sess.App.Uuid
	node.Cstat = cf.K_STAT_WAIT
	node.IsExit = false
	node.defaultv = cf.FuncEmptyRet(node.Func) // FIXME: need custom
	cf.Assert(node.defaultv != nil, "default return value is nil")
	return &node
}

func (node *Node) Update(kw ...map[string]interface{}) {
	var node_rf = reflect.ValueOf(node)
	for _, arg := range kw {
		for key, value := range arg {
			v := node_rf.Elem().FieldByName(key)
			if v.CanSet() {
				v.Set(reflect.ValueOf(value))
			} else {
				cf.Assert(false, "Set Node fail: k=%s v=%s, %s", key, value, kw)
			}
		}
	}
	identies := cf.DotS(node.Flow.Uuid, cf.Itos(node.Idx), node.Name, cf.Itos(node.SubIdx))
	node.Uuid = cf.AsMd5(identies)
}

func (node *Node) append(fc interface{}, name string, ex_args []interface{}) *Node {
	var new_node = NewNode(node.Flow, map[string]interface{}{
		"Name":     name,
		"Func":     fc,
		"ExArgs":   ex_args,
		"InsCount": 1,
	})
	new_node.PreNodes = append(new_node.PreNodes, node)
	node.NexNodes = append(node.NexNodes, new_node)
	new_node.Update()
	node.Flow.AddNode(new_node)
	return new_node
}

func (node *Node) Add(fc interface{}, name string, ex_args ...interface{}) *Node {
	return node.append(fc, name, ex_args)
}

func (node *Node) Map(fc interface{}, name string, count int, ex_args ...interface{}) *Node {
	var new_node = node.append(fc, name, ex_args)
	new_node.InsCount = count
	return new_node
}

func (node *Node) Reduce(fc interface{}, name string, batch int, ex_args ...interface{}) *Node {
	v := node.append(fc, name, ex_args)
	v.Batch = batch
	return v
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
}

func (node *Node) Call(a []interface{}) []interface{} {
	args := []interface{}{node}
	args = append(args, a...)
	args = append(args, node.ExArgs...)
	node.PreCall()
	ret := cf.FuncCall(node.Func, args)
	if node.Exited() {
		node.callCount -= 1
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
	in_ch := []string{}
	for _, n := range node.PreNodes {
		in_ch = append(in_ch, cf.DotS(n.Uuid, "out"))
	}
	// uuid-0 => uuid
	return in_ch, []string{cf.DotS(strings.Split(node.Uuid, "-")[0], "out")}
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

func (node *Node) GetExitChs() (map[string][]interface{}, bool) {
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
			ch_val[cf.DotS(n.Uuid, "out")] = n.defaultv
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
				ch_val[cf.DotS(uuid, "out")] = n.defaultv
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

func (node *Node) IgnoreRet() bool {
	if len(node.NexNodes) > 0 {
		return false
	}
	return true
}

func (node *Node) AsTask() task.Task {
	return task.Task{
		List_key: cf.DotS(node.Parent, cf.K_AB_NODE),
		Uuid_key: cf.DotS(cf.K_AB_NODE, node.Uuid),
	}
}

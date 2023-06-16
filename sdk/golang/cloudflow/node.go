package cloudflow

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

type Node struct {
	IsExit   bool          `json:"-"`
	Name     string        `json:"name"`
	Func     interface{}   `json:"-"`
	Flow     *Flow         `json:"-"`
	Uuid     string        `json:"uuid"`
	Idx      int           `json:"index"`
	SubIdx   int           `json:"subidx"`
	PreNodes []*Node       `json:"-"`
	NexNodes []*Node       `json:"-"`
	ExArgs   []interface{} `json:"-"`
	Synchz   bool          `json:"synchz"`
	InsCount int           `json:"inscount"`
	CTime    int64         `json:"ctime"`
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
		IsExit: false,
		Idx:    __node_index__,
		Flow:   flow,
		CTime:  cf.Timestamp(),
	}
	__node_index__ += 1
	node.Update(kw...)
	node.Parent = "flow." + flow.Uuid
	node.AppUid = flow.Sess.App.Uuid
	node.Cstat = cf.K_STAT_WAIT
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
		"Synchz":   false,
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

func (node *Node) Reduce(fc interface{}, name string, sync bool, ex_args ...interface{}) *Node {
	v := node.append(fc, name, ex_args)
	v.Synchz = sync
	return v
}

func (node *Node) GetSession() *Session {
	return node.Flow.Sess
}

package cloudflow

import (
	"reflect"
	"fmt"
	"encoding/json"
	"runtime"
	"strings"
)

type Node struct {
	Name     string        `json:"name"`
	Func     interface{}   `json:"-"`
	Flow     *Flow         `json:"-"`
	Uuid     string        `json:"uuid"`
	Idx      int           `json:"index"`
	SubIdx   int           `json:"subidx"`
	OutCh    string        `json:"outch"`
	PreNodes []*Node       `json:"-"`
	NexNodes []*Node       `json:"-"`
	KWArgs   []interface{} `json:"-"`
	Synchz   bool          `json:"synchz"`
	InsCount int           `json:"inscount"`
	CTime    int64         `json:"ctime"`
	ATime    int64         `json:"atime"`
}


func (node *Node) MarshalJSON() ([]byte, error) {
	type JNode Node
	func_name := strings.Replace(reflect.ValueOf(node.Func).String(), "func(", 
	                             runtime.FuncForPC(reflect.ValueOf(node.Func).Pointer()).Name()+"(", 1)
	return json.Marshal(&struct{
		*JNode
		Func string `json:"func"`
	}{
		JNode: (*JNode)(node),
		Func: func_name,
	})
}


func (node *Node) String() string {
	return fmt.Sprintf("Node(%s, %s)", node.Uuid, node.Name)
}


var __node_index__ int = 0
func NewNode(flow *Flow, kw... map[string]interface{}) *Node{
	var node = Node{
		Idx: __node_index__,
		Flow: flow,
		CTime: Timestamp(),
	}
	__node_index__ += 1
	node.Update(kw...)
	return &node
}


func (node *Node)Update(kw... map[string]interface{}){
	var node_rf = reflect.ValueOf(node)
	for _, arg := range kw{
		for key, value := range arg {
			v := node_rf.Elem().FieldByName(key)
			if v.CanSet() {
				v.Set(reflect.ValueOf(value))
			} else {
				Err("Set Node fail:", "k=", key, "v=", value)
			}
		}
	}
	outch     := node.Flow.Uuid + "." + Itos(node.Idx) + "." + node.Name
	node.OutCh = outch
	identies  := outch + "." + Itos(node.SubIdx)
	node.Uuid  = AsMd5(identies)
}


func (node *Node)append(fc interface{}, name string, kwargs[]interface{}) *Node{
	var new_node = NewNode(node.Flow, map[string]interface{}{
		"Name":     name,
		"Func":     fc,
		"KWArgs":   kwargs,
		"InsCount": 1,
		"Synchz":   false,
	})
	new_node.PreNodes = append(new_node.PreNodes, node)
	node.NexNodes = append(node.NexNodes, new_node)
	new_node.Update()
	node.Flow.AddNode(new_node)
	return new_node
}


func (node *Node )Add(fc interface{}, name string, kwargs... interface{}) *Node{
	return node.append(fc, name, kwargs)
}


func (node *Node )Map(fc interface{}, name string, count int, kwargs... interface{}) *Node{
	var new_node = node.append(fc, name, kwargs)
	new_node.InsCount = count
	return new_node
}


func (node *Node )Reduce(fc interface{}, name string, kwargs... interface{}) *Node{
	return node.append(fc, name, kwargs)
}

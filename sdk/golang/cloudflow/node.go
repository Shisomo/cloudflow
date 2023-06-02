package cloudflow

import (
	"reflect"
	"fmt"
)

type Node struct {
	Name     string
	Func     interface{}
	Flow     *Flow
	Uuid     string
	Idx      int
	SubIdx   int
	OutCh    string
	PreNodes []*Node
	NexNodes []*Node
	KWArgs   []interface{}
	Synchz   bool
	InsCount int
}

func (node *Node) String() string {
	return fmt.Sprintf("Node(%s, %s)", node.Uuid, node.Name)
}

var __node_index__ int = 0
func NewNode(flow *Flow, kw... map[string]interface{}) *Node{
	var node = Node{
		Idx: __node_index__,
		Flow: flow,
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
				Err("Set Node.%s with value: %s fail", key, value)
			}
		}
	}
	identies := node.Name + Itos(node.Idx) + "-" + Itos(node.SubIdx) + "-" + node.Flow.Uuid
	node.Uuid = AsMd5(identies)
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

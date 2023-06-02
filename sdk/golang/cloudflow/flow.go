package cloudflow

import (
	"fmt"
)

type Flow struct {
	Name  string
	Uuid  string
	Sess  *Session
	Nodes []*Node
	Idx   int
}

var __flow_index__ int = 0
func NewFlow(se *Session, name string) *Flow{
	flow := Flow{
		Name: name,
		Uuid: AsMd5(se.Uuid + Itos(__flow_index__)),
		Sess: se,
		Idx:  __flow_index__,
	}
	__flow_index__ += 1
	return &flow
}

func (f *Flow) String() string{
	return fmt.Sprintf("Fow(%s, %s)", f.Uuid, f.Name)
}


func (flow *Flow )Add(fc interface{}, name string, kwargs... interface{}) *Node{
	var new_node = NewNode(flow, map[string]interface{}{
		"Name":     name,
		"Func":     fc,
		"KWArgs":   kwargs,
		"InsCount": 1,
		"Synchz":   false,
	})
	flow.AddNode(new_node)	
	return new_node
}


func (flow *Flow) AddNode(node *Node) {
	flow.Nodes = append(flow.Nodes, node)
}


func (flow *Flow)DrawTxt() string{
	node_count := len(flow.Nodes)
	fmt_str := fmt.Sprintf("\n%s size=%d: \nNodes:\n", flow, node_count)
	for index, node := range flow.Nodes {
		node_fmt := If(index < node_count -1, "  %s\n", "  %s").(string)
		fmt_str += fmt.Sprintf(node_fmt, node)
	}
	return fmt_str
}


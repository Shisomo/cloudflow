package cloudflow

import (
	"fmt"
)

type Flow struct {
	Name  string    `json:"name"`
	Uuid  string    `json:"uuid"`
	Sess  *Session  `json:"-"`
	Nodes []*Node   `json:"nodes"`
	Idx   int       `json:"index"`
	CTime int64     `json:"ctime"`
}

var __flow_index__ int = 0
func NewFlow(se *Session, name string) *Flow{
	flow := Flow{
		Name: name,
		Uuid: AsMd5(se.Uuid + Itos(__flow_index__)),
		Sess: se,
		Idx:  __flow_index__,
		CTime: Timestamp(),
		Nodes: []*Node{},
	}
	__flow_index__ += 1
	se.Flows = append(se.Flows, &flow)
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

package cloudflow

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"fmt"
)

type Flow struct {
	Name  string   `json:"name"`
	Uuid  string   `json:"uuid"`
	Sess  *Session `json:"-"`
	Nodes []*Node  `json:"node"`
	Idx   int      `json:"index"`
	CTime int64    `json:"ctime"`
	cf.CommStat
}

var __flow_index__ int = 0

func NewFlow(se *Session, name string) *Flow {
	flow := Flow{
		Name:  name,
		Uuid:  cf.AsMd5(se.Uuid + cf.Itos(__flow_index__)),
		Sess:  se,
		Idx:   __flow_index__,
		CTime: cf.Timestamp(),
		Nodes: []*Node{},
	}
	__flow_index__ += 1
	se.Flows = append(se.Flows, &flow)
	flow.Parent = "sess." + se.Uuid
	return &flow
}

func (f *Flow) String() string {
	return fmt.Sprintf("Fow(%s, %s)", f.Uuid, f.Name)
}

func (flow *Flow) Add(fc interface{}, name string, args ...interface{}) *Node {
	ex_args, options := ParsOptions(args...)
	var new_node = NewNode(flow, map[string]interface{}{
		"Name":     name,
		"Func":     fc,
		"ExArgs":   ex_args,
		"InsCount": 1,
	})
	new_node.Update(options)
	flow.AddNode(new_node)
	return new_node
}

func (flow *Flow) AddNode(node *Node) {
	flow.Nodes = append(flow.Nodes, node)
}

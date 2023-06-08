package internal

import (
	sr "cloudflow/internal/service"
	cf "cloudflow/sdk/golang/cloudflow"
)

type CloudFlow struct {
	cfg      *map[string]interface{}
	StateSrv sr.StateOps
}

func NewCloudFlow(cfg *map[string]interface{}) *CloudFlow {
	return &CloudFlow{
		cfg: cfg,
	}
}

func (self *CloudFlow) StartService() {
	cfg := self.cfg
	cf.Log("start cf.state")
	srv_state := sr.GetStateImp(cf.GetCfg(cfg, "cf.services.state").(map[string]interface{}))
	cf.Assert(srv_state.Restart(), "start cf.services fail")
	self.StateSrv = srv_state.(sr.StateOps)

	cf.Log("start cf.message")
	srv_message := sr.GetMessageImp(cf.GetCfg(cfg, "cf.services.message").(map[string]interface{}))
	cf.Assert(srv_message.Restart(), "start cf.message fail")

	// start kv service
	cf.Log("Fake start kv service, FIXME")

	// start file storage
	cf.Log("Fake start kv service, FIXME")
}

func (self *CloudFlow) Schedule() {
	cf.Log("Schedule FIXME")
}

func (self *CloudFlow) SubmitApp(app_id string, app_base64_cfg string, exec_file string, node_uuid string) {
	cf.Log("submit app:", app_id, "exec:", exec_file, "node:", node_uuid)
	cf.Log(self.StateSrv.Get("cl-app-list"))
}

var version = "0.1"

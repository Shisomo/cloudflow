package internal

import (
	"cloudflow/internal/schedule"
	sr "cloudflow/internal/service"
	"cloudflow/internal/worker"
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
	srv_state := sr.GetStateImp(cf.GetCfgC(cfg, "cf.services.state"))
	cf.Assert(srv_state.Restart(), "start cf.services fail")
	self.StateSrv = srv_state.(sr.StateOps)

	cf.Log("start cf.message")
	srv_message := sr.GetMessageImp(cf.GetCfgC(cfg, "cf.services.message"))
	cf.Assert(srv_message.Restart(), "start cf.message fail")

	// start kv service
	cf.Log("Fake start kv service, FIXME")
	// TBD

	// start file storage
	cf.Log("Fake start kv service, FIXME")
	// TBD

	// check scheduler, if no one, start dumy scheduler
	schedule.TryStartSchduler(cf.GetCfgC(cfg, "cf.scheduler"), self.StateSrv)

	// check worker, if no one, start dumy worker
	worker.TryStartWorker(cf.GetCfgC(cfg, "cf.worker"), self.StateSrv)

}

func (self *CloudFlow) SubmitApp(app_id string, app_base64_cfg string, exec_file string, node_uuid string) {
	cf.Log("submit app:", app_id, "exec:", exec_file, "node:", node_uuid)
	cf.Log(self.StateSrv.Get("cl-app-list"))
	// ....
	// collect logs and wait app run complete
	// TBD
}

var version = "0.1"

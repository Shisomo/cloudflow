package internal

import (
	"cloudflow/internal/schedule"
	sr "cloudflow/internal/service"
	"cloudflow/internal/worker"
	"cloudflow/sdk/golang/cloudflow/chops"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/fileops"
	"cloudflow/sdk/golang/cloudflow/kvops"
)

type CloudFlow struct {
	cfg *cf.CFG
	// services
	StateSrv  sr.StateService
	MessagSrv sr.MessageService
	// operations
	MsgOps  chops.ChannelOp
	StatOps kvops.KVOp
	FileOps fileops.FileOps
}

func NewCloudFlow(cfg *cf.CFG) *CloudFlow {
	return &CloudFlow{
		cfg:      cfg,
		StateSrv: nil,
		MsgOps:   nil,
		StatOps:  nil,
		FileOps:  nil,
	}
}

func (self *CloudFlow) ConnectMsg(scope ...string) {
	if self.MsgOps == nil {
		cfg := cf.GetCfgC(self.cfg, cf.CFG_KEY_SRV_MESSAGE)
		if len(scope) > 0 {
			cfg["app_id"] = scope[0]
		} else {
			cfg["app_id"] = scope
		}
		self.MsgOps = chops.GetChOpsImp(cfg)
	}
}

func (self *CloudFlow) ConnectStat() {
	if self.StatOps == nil {
		self.StatOps = kvops.GetKVOpImp(cf.GetCfgC(self.cfg, cf.CFG_KEY_SRV_STATE))
	}
}

func (self *CloudFlow) Connect() {
	self.ConnectStat()
	self.ConnectMsg()
}

func (self *CloudFlow) ConnectFile(app_key string) {
	if self.FileOps == nil {
		self.FileOps = fileops.GetFileOps(app_key, cf.GetCfgC(self.cfg, cf.CFG_KEY_SRV_FSTORE))
	}
}

func (self *CloudFlow) StartService() {
	cfg := self.cfg
	cf.Log("start service state")
	if self.StateSrv == nil {
		srv_state := sr.GetStateImp(cf.GetCfgC(cfg, cf.CFG_KEY_SRV_STATE))
		cf.Assert(srv_state.Restart(), "start stat service fail")
		self.StateSrv = srv_state.(sr.StateService)
		self.StatOps = self.StateSrv.GetKVOps()
	}
	if self.MessagSrv == nil {
		cf.Log("start service message")
		srv_message := sr.GetMessageImp(cf.GetCfgC(cfg, cf.CFG_KEY_SRV_MESSAGE))
		cf.Assert(srv_message.Restart(), "start message service fail")
		self.MessagSrv = srv_message.(sr.MessageService)
		self.MsgOps = self.MessagSrv.GetChannelOps()
	}
	// start kv service
	cf.Log("Fake start kv service, FIXME")
	// TBD

	// start file storage
	cf.Log("Fake start file storage service, FIXME")
	// TBD
}

func (self *CloudFlow) StartSchAndWorker() {
	// check scheduler, if no one, start dumy scheduler
	schedule.TryStartSchduler(cf.GetCfgC(self.cfg, cf.CFG_KEY_SRV_SCEDULER),
		self.StatOps)
	// check worker, if no one, start dumy worker
	worker.TryStartWorker(cf.GetCfgC(self.cfg, cf.CFG_KEY_SRV_WORKER),
		cf.GetCfgC(self.cfg, cf.CFG_KEY_SRV_FSTORE), self.StatOps)
}

var Version = cf.Version()

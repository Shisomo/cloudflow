package internal

import (
	"cloudflow/internal/schedule"
	sr "cloudflow/internal/service"
	"cloudflow/internal/worker"
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	"cloudflow/sdk/golang/cloudflow/chops"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/fileops"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"strings"
	"time"
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

func (self *CloudFlow) SubmitApp(app_key string, app_base64_cfg string, exec_file string, app_args string, node_uuid string) {
	cf.Log("submit app:", app_key, "exec:", exec_file, "node:", node_uuid)
	cf.Log("find apps:", cfmodule.ListKeys(self.StatOps, cf.K_CF_APPLIST, ""))
	app_id := strings.Split(app_key, ".")[1]
	if !cf.StrListHas(cfmodule.ListKeys(self.StatOps, cf.K_CF_APPLIST, ""), app_id) {
		cf.Log("load app:", app_id)
		self.StatOps.Set(cf.DotS(cf.K_CF_APPLIST, app_key), cf.K_STAT_WAIT)

		exec_file_key := cf.DotS(app_key, cf.K_MEMBER_EXEC)
		exec_app_args := cf.DotS(app_key, cf.K_MEMBER_APPARGS)

		self.StatOps.Set(exec_file_key, exec_file)
		self.StatOps.Set(exec_app_args, app_args)
		self.StatOps.Set(cf.DotS(app_key, cf.K_MEMBER_RUNCFG), cf.Base64En(cf.AsJson(self.cfg)))

		app_data := cf.Json2Map(cf.Base64De(app_base64_cfg))
		self.StatOps.SetKV(app_data, false)

		// upload exec file
		cf.Log("start file storage service")
		self.FileOps = fileops.GetFileOps(app_key, cf.GetCfgC(self.cfg, cf.CFG_KEY_SRV_FSTORE))
		self.FileOps.Put(exec_file_key, exec_file)
		self.FileOps.Close()
	} else {
		cf.Log("app:", app_id, "already exist")
	}

	// watch app logs
	ch := []string{
		app_id + ".log",
	}
	self.ConnectMsg(app_id)
	self.MsgOps.Watch(app_id, ch, func(worker string, sub string, data string) bool {
		cf.Log(worker, data)
		return false
	})
	cf.Log("waiting logs from:", ch)
	for {
		if self.StatOps.Get(cfmodule.GetStat(self.StatOps, app_key)) == cf.K_STAT_EXIT {
			cf.Log("run app:", app_id, " complete")
			break
		}
		time.Sleep(5 * time.Second)
	}
}

var Version = cf.Version()

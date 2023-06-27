package internal

import (
	"cloudflow/internal/schedule"
	sr "cloudflow/internal/service"
	"cloudflow/internal/worker"
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	"cloudflow/sdk/golang/cloudflow/chops"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/fileops"
	"strings"
	"time"
)

type CloudFlow struct {
	cfg      *cf.CFG
	StateSrv sr.StateOps
	chOps    chops.ChannelOp
	flOps    fileops.FileOps
}

func NewCloudFlow(cfg *cf.CFG) *CloudFlow {
	return &CloudFlow{
		cfg: cfg,
	}
}

func NewChanlOps(cfg cf.CFG, stream string) chops.ChannelOp {
	imp := cfg["imp"].(string)
	host := cfg["host"].(string)
	port := cfg["port"].(int)
	switch imp {
	case "nats":
		return chops.NewNatsChOp("nats://"+host+":"+cf.Itos(port), stream)
	default:
		cf.Assert(false, "%s not support", imp)
	}
	return nil
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
	cf.Log("Fake start file storage service, FIXME")
	// TBD
}

func (self *CloudFlow) StartSchAndWorker() {
	// check scheduler, if no one, start dumy scheduler
	schedule.TryStartSchduler(cf.GetCfgC(self.cfg, "cf.scheduler"), self.StateSrv)
	// check worker, if no one, start dumy worker
	worker.TryStartWorker(cf.GetCfgC(self.cfg, "cf.worker"), cf.GetCfgC(self.cfg, "cf.services.fstore"), self.StateSrv)
}

func (self *CloudFlow) SubmitApp(app_key string, app_base64_cfg string, exec_file string, app_args string, node_uuid string) {
	cf.Log("submit app:", app_key, "exec:", exec_file, "node:", node_uuid)
	cf.Log("find apps:", cfmodule.ListKeys(self.StateSrv, cf.K_CF_APPLIST, ""))
	app_id := strings.Split(app_key, ".")[1]
	if !cf.StrListHas(cfmodule.ListKeys(self.StateSrv, cf.K_CF_APPLIST, ""), app_id) {
		cf.Log("load app:", app_id)
		self.StateSrv.Set(cf.DotS(cf.K_CF_APPLIST, app_key), cf.K_STAT_WAIT)

		exec_file_key := cf.DotS(app_key, cf.K_MEMBER_EXEC)
		exec_app_args := cf.DotS(app_key, cf.K_MEMBER_APPARGS)

		self.StateSrv.Set(exec_file_key, exec_file)
		self.StateSrv.Set(exec_app_args, app_args)
		self.StateSrv.Set(cf.DotS(app_key, cf.K_MEMBER_RUNCFG), cf.Base64En(cf.AsJson(self.cfg)))

		app_data := cf.Json2Map(cf.Base64De(app_base64_cfg))
		self.StateSrv.SetKV(app_data, false)

		// upload exec file
		cf.Log("start file storage service")
		self.flOps = fileops.GetFileOps(app_key, cf.GetCfgC(self.cfg, "cf.services.fstore"))
		self.flOps.Put(exec_file_key, exec_file)
		self.flOps.Close()
	} else {
		cf.Log("app:", app_id, "already exist")
	}

	// watch app logs
	self.chOps = NewChanlOps(cf.GetCfgC(self.cfg, "cf.services.message"), app_id)
	ch := []string{
		app_id + ".log",
	}
	self.chOps.Watch(app_id, ch, func(worker string, sub string, data string) bool {
		cf.Log(worker, data)
		return false
	})
	cf.Log("waiting logs from:", ch)
	for {
		if self.StateSrv.Get(cfmodule.GetStat(self.StateSrv, app_key)) == cf.K_STAT_EXIT {
			cf.Log("run app:", app_id, " complete")
			break
		}
		time.Sleep(5 * time.Second)
	}
}

var version = "0.1"

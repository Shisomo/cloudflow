package internal

import (
	"cloudflow/internal/schedule"
	sr "cloudflow/internal/service"
	"cloudflow/internal/worker"
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	"cloudflow/sdk/golang/cloudflow/chops"
	"strings"
	"time"
)

type CloudFlow struct {
	cfg      *map[string]interface{}
	StateSrv sr.StateOps
	chOps    chops.ChannelOp
}

func NewCloudFlow(cfg *map[string]interface{}) *CloudFlow {
	return &CloudFlow{
		cfg: cfg,
	}
}

func NewChanlOps(cfg map[string]interface{}, stream string) chops.ChannelOp {
	imp := cfg["imp"].(string)
	host := cfg["host"].(string)
	port := cfg["port"].(int)
	switch imp {
	case "nats":
		return chops.NewNatsChOp("nats://"+host+cf.Itos(port), stream)
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
	cf.Log("Fake file storage service, FIXME")
	// TBD
}

func (self *CloudFlow) StartSchAndWorker() {
	// check scheduler, if no one, start dumy scheduler
	schedule.TryStartSchduler(cf.GetCfgC(self.cfg, "cf.scheduler"), self.StateSrv)
	// check worker, if no one, start dumy worker
	worker.TryStartWorker(cf.GetCfgC(self.cfg, "cf.worker"), self.StateSrv)
}

func (self *CloudFlow) SubmitApp(app_key string, app_base64_cfg string, exec_file string, node_uuid string) {
	cf.Log("submit app:", app_key, "exec:", exec_file, "node:", node_uuid)
	cf.Log(self.StateSrv.Get("cl-app-list"))

	app_id := strings.Split(app_key, ".")[1]
	if !cf.StrListHas(cfmodule.ListCfModule(self.StateSrv, cf.K_CF_APPLIST), app_id) {
		cf.Log("load app:", app_id)
		cfmodule.ListAdd(self.StateSrv, cf.K_CF_APPLIST, app_id, "user_submit", true)
		app_data := cf.Json2Map(cf.Base64De(app_base64_cfg))
		self.StateSrv.SetKV(app_data, false)
	} else {
		cf.Log("app:", app_id, "already exist")
	}

	// wach logs
	self.chOps = NewChanlOps(cf.GetCfgC(self.cfg, "cf.services.message"), app_id)
	ch := []string{
		app_id + ".log",
	}
	self.chOps.Watch(ch, func(worker string, sub string, data string) bool {
		cf.Log(worker, sub, data)
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

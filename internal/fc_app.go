package internal

import (
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/fileops"
	"strings"
	"time"
)

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
		cf.Info(worker, data)
		return false
	})
	cf.Info("waiting logs from:", ch)
	for {
		if self.StatOps.Get(cfmodule.GetStat(self.StatOps, app_key)) == cf.K_STAT_EXIT {
			cf.Info("run app:", app_id, " complete")
			break
		}
		time.Sleep(5 * time.Second)
	}
}

func (self *CloudFlow) ListApp() [][]string {
	ret := [][]string{}
	// fmt:
	//   appid  appname  ctime  atime  nodes  stat
	//   ****   *****    ****   ****   ****   ****
	for _, app_id := range cfmodule.ListKeys(self.StatOps, cf.K_CF_APPLIST, "") {
		app_info := self.StatOps.Get(cf.DotS(cf.K_AB_CFAPP, app_id, "*")).(map[string]interface{})
		app_name := app_info[cf.DotS(cf.K_AB_CFAPP, app_id, "name")].(string)
		app_ctime := cf.TimeFmt(app_info[cf.DotS(cf.K_AB_CFAPP, app_id, "ctime")].(float64))
		app_atime := cf.TimeFmt(app_info[cf.DotS(cf.K_AB_CFAPP, app_id, "name")].(float64))
		app_nodes := cf.Astr(app_info[cf.DotS(cf.K_AB_CFAPP, app_id, "nodes")])
		app_stat := app_info[cf.DotS(cf.K_AB_CFAPP, app_id, "state")].(string)
		ret = append(ret,
			[]string{app_id, app_name, app_ctime, app_atime, app_nodes, app_stat},
		)
	}
	return ret
}

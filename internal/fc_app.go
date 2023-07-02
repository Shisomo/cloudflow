package internal

import (
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	cf "cloudflow/sdk/golang/cloudflow/comm"
)

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

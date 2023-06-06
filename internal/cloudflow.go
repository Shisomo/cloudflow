package internal

import (
	sr "cloudflow/internal/service"
	cf "cloudflow/sdk/golang/cloudflow"
)

func StartService(cfg *map[string]interface{}){
	cf.Log("start cf.state")
	srv_state := cf.GetCfg(cfg, "cf.services.state").(map[string]interface{})
	cf.Assert(sr.GetStateSvr(srv_state).Restart(), "start cf.services fail")
	
	cf.Log("start cf.message")
	// start cfg service
	// start kv service
	// start file storage
}


func Schedule(cfg *map[string]interface{}){
	// TBD
}


func SubmitApp(cfg *map[string]interface{}, app_id string, app_base64_cfg string, exec_file string, node_uuid string){
	// start all services
	// load all nodes
}


var version = "0.1"
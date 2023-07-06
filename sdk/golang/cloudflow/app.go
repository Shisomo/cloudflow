package cloudflow

import (
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	"cloudflow/sdk/golang/cloudflow/chops"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"cloudflow/sdk/golang/cloudflow/task"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type App struct {
	cfg  cf.CFG     `json:"-"`
	Name string     `json:"name"`
	Uuid string     `json:"uuid"`
	Svrs []*Service `json:"srvs"`
	Sess []*Session `json:"sess"`
	cf.CommStat
}

func (app *App) CreateSession(name string) *Session {
	return NewSession(app, name)
}

func (app *App) Reg(fc interface{}, name string, ex_args ...interface{}) *App {
	NewService(app, fc, name, ex_args)
	return app
}

func (app *App) getNode(key string) (string, int, RunInterface) {
	// key eg: node.b6673ac7ba225246020795ab50b8a770-1
	ks := strings.Split(key, ".")
	ntype := ks[0]
	uuid := ks[1]
	subindex := 0
	if strings.Contains(uuid, "-") {
		v := strings.Split(uuid, "-")
		uuid = v[0]
		index, err := strconv.Atoi(v[1])
		cf.Assert(err == nil, "convert subint(%s) error: %s", v[1], err)
		subindex = index
	}
	if ntype == cf.K_AB_SERVICE {
		for _, s := range app.Svrs {
			if s.Uuid == uuid {
				return ntype, subindex, s
			}
		}
	}
	if ntype == cf.K_AB_NODE {
		for _, s := range app.Sess {
			for _, f := range s.Flows {
				for _, n := range f.Nodes {
					if n.Uuid == uuid {
						return ntype, subindex, n
					}
				}
			}
		}
	}
	return ntype, subindex, nil
}

func (app *App) runNode() {
	time_start := cf.Timestamp()
	cf.LogSetPrefix("<" + cf.EnvNodeUuid() + "> ")
	cf.Log("run node with args:", cf.EnvAPPHost(), cf.EnvAPPPort(), cf.EnvAPPUuid(), cf.EnvNodeUuid())
	app_id := cf.EnvAPPUuid()
	node_key := cf.EnvNodeUuid()

	ntype, subindex, ins := app.getNode(node_key)
	cf.Assert(ins != nil, "Node(%s) not find", node_key)
	statops := kvops.GetKVOpImp(map[string]interface{}{
		"host":  cf.EnvAPPHost(),
		"port":  cf.EnvAPPPort(),
		"scope": cf.EnvAPPScope(),
		"imp":   cf.EnvAPPIMP(),
	})
	ins.SetKVOps(statops)
	ins.UpdateUUID(node_key)

	runcfg_tp := cf.FrJson(cf.Base64De(statops.Get(cf.DotS(cf.K_AB_CFAPP, app_id, cf.K_MEMBER_RUNCFG)).(string))).(map[string]interface{})
	runcfg := cf.ConvertoCFG(&runcfg_tp)

	ch_cfg := cf.GetCfgC(&runcfg, "cf.services.message")
	ch_cfg["app_id"] = app_id
	msgops := chops.GetChOpsImp(ch_cfg)
	CheckNodeSrvInsCount(statops, ntype, node_key, subindex, ins)
	ins.SetMsgOps(msgops)

	// run node
	msg_index := ins.Run()
	// update task state
	ins.SyncState()
	task.UpdateStat(statops, ins.AsTask(), cf.K_STAT_EXIT, ins.UUID())
	time_end := cf.Timestamp()
	ins.MsgLogf("%s-%s exit (recv: %d, send: %d) cost %ds", ins.GetName(), ins.FuncName(), ins.CallCount(),
		msg_index, (time_end-time_start)/int64(time.Second))
}

func CheckNodeSrvInsCount(ops kvops.KVOp, ntype string, node_key string, subindex int, ins RunInterface) {
	instance_count := int(cfmodule.GetVal(ops, node_key, cf.K_MEMBER_INSCOUNT).(float64))
	cf.Assert(ins.InstanceCount() == instance_count, "error instance count not persistent %d != %d", ins.InstanceCount(), instance_count)
	ins.SetSubIdx(subindex)
}

func NewApp(name string, cfg ...cf.CFG) *App {
	app_uid := cf.EnvAPPUuid()
	app_uid = cf.If(app_uid != "", app_uid, cf.AsMd5(cf.TimestampStr())).(string)
	app := App{
		Name: name,
		Uuid: app_uid,
		Svrs: []*Service{},
		Sess: []*Session{},
		cfg:  cf.MergeCFG(cfg...),
	}
	app.CTime = cf.Timestamp()
	return &app
}

func (app *App) CompareJson(jdata string) bool {
	new_app := App{}
	if err := json.Unmarshal([]byte(jdata), &new_app); err != nil {
		return false
	}
	for sdx, sess := range new_app.Sess {
		uuid_sess := app.Sess[sdx].Uuid
		cf.Assert(sess.Uuid == uuid_sess, "Check Session[%s] fail: session:%d != %s", sess.Uuid, sdx, uuid_sess)
		for fdx, flow := range sess.Flows {
			uuid_flow := app.Sess[sdx].Flows[fdx].Uuid
			cf.Assert(flow.Uuid == uuid_flow, "Check flow[%s] fail: session:%d-flow:%d != %s", flow.Uuid, sdx, fdx, uuid_flow)
			for ndx, node := range flow.Nodes {
				uuid_node := app.Sess[sdx].Flows[fdx].Nodes[ndx].Uuid
				cf.Assert(node.Uuid == uuid_node,
					"check Node(%s) fail: session:%d-flow:%d-Node:%d != %s", node.Uuid, sdx, fdx, ndx, uuid_node)
			}
		}
	}
	return true
}

func (app *App) ExportJson() string {
	enc, err := json.MarshalIndent(app, "", " ")
	if err != nil {
		cf.Err(err)
	}
	jdata := string(enc)
	cf.Assert(app.CompareJson(jdata), "export compare fail!")
	return jdata
}

func (app *App) ExportConfigJson() (string, string) {
	var appdata map[string]interface{}
	cfgjson := app.ExportJson()
	json.Unmarshal([]byte(cfgjson), &appdata)

	var exportJS = make(map[string]interface{})
	prefix := cf.DotS(cf.K_AB_CFAPP, app.Uuid)
	exportJS[prefix+".rawcfg"] = cf.Base64En(cfgjson)
	exportJS[prefix+".sdkv"] = cf.Version()
	cf.DumpKV(&appdata, &exportJS, prefix, "uuid", "cstat")

	return prefix, cf.AsJson(&exportJS)
}

func (app *App) Run() {
	if cf.EnvAPPUuid() != "" && cf.EnvNodeUuid() != "" {
		app.runNode()
		return
	}
	path_to_seek := []string{
		"cloudflow.bash",
		"script/cloudflow.bash",
		"cf",
		"bin/cf",
	}
	cf_path := ""
	for _, path := range path_to_seek {
		_, err := os.Stat(path)
		if err == nil {
			cf.Log("find cloudflow:", path)
			cf_path = path
			break
		}
	}
	if cf_path == "" {
		path, err := exec.LookPath("cf")
		cf.Assert(err == nil, "cannot find cloudflow launcher")
		cf_path = path
	}
	app_id, app_cfg := app.ExportConfigJson()

	// construct args
	opts := []string{
		cf_path, "run", app_id, cf.Base64En(app_cfg), os.Args[0], cf.Base64En(strings.Join(os.Args[1:], " ")),
	}
	host := cf.EnvAPPHost()
	port := cf.EnvAPPPort()
	cfg_host, ex := app.cfg["host"]
	if ex {
		host = cfg_host.(string)
	}
	cfg_port, es := app.cfg["port"]
	if es {
		port = cfg_port.(string)
	}
	if host != "" {
		opts = append(opts, "-H", cf.Astr(host))
	}
	if port != "" {
		opts = append(opts, "-p", cf.Astr(port))
	}
	cmd := exec.Command("bash", opts...)
	cf.Log("*************cloudflow output*************")
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err := cmd.Start()
	cf.Assert(err == nil, "launch cf fail: %s", err)
	cmd.Wait()
	cf.Log("Exit App")
}

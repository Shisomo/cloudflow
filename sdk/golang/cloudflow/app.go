package cloudflow

import (
	"cloudflow/sdk/golang/cloudflow/chops"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type App struct {
	cfg   cf.CFG     `json:"-"`
	Name  string     `json:"name"`
	Uuid  string     `json:"uuid"`
	Svrs  []*Service `json:"srvs"`
	Sess  []*Session `json:"sess"`
	CTime int64      `json:"ctime"`
	cf.CommStat
}

func (app *App) CreateSession(name string) *Session {
	return NewSession(app, name)
}

func (app *App) Reg(fc interface{}, name string, ex_args ...interface{}) *App {
	NewService(app, fc, name, ex_args)
	return app
}

func (app *App) getInOutCh(kv kvops.KVOp, ntype string, subindex int, ins interface{}) ([]string, []string, interface{}, []interface{}) {
	chs_i := []string{}
	chs_o := []string{}
	var fc interface{}
	var ex_args []interface{}

	switch ntype {
	case cf.K_AB_NODE:
		node := ins.(*Node)
		for _, n := range node.PreNodes {
			ins_count := int(kv.Get(cf.DotS(cf.K_AB_NODE, n.Uuid, cf.K_MEMBER_INSCOUNT)).(float64))
			for i := 0; i < ins_count; i++ {
				chs_i = append(chs_i, cf.DotS("out", n.Uuid, cf.Astr(i)))
			}
		}
		chs_o = append(chs_o, cf.DotS("out", node.Uuid, cf.Astr(subindex)))
		fc = node.Func
		ex_args = node.ExArgs
	case cf.K_AB_SERVICE:
		srv := ins.(*Service)
		chs_i = append(chs_i, cf.DotS("in", srv.Uuid))
		chs_o = append(chs_o, cf.DotS("out", srv.Uuid))
		fc = srv.Func
		ex_args = srv.ExArgs
	default:
		cf.Assert(false, "%s not supported", ntype)
	}
	return chs_i, chs_o, fc, ex_args
}

func (app *App) getNode(key string) (string, int, interface{}, interface{}) {
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
				return ntype, subindex, s, app
			}
		}
	}
	if ntype == cf.K_AB_NODE {
		for _, s := range app.Sess {
			for _, f := range s.Flows {
				for _, n := range f.Nodes {
					if n.Uuid == uuid {
						return ntype, subindex, n, s
					}
				}
			}
		}
	}
	return ntype, subindex, nil, nil
}

func (app *App) runNode() {
	cf.LogSetPrefix("<" + cf.EnvNodeUuid() + "> ")

	cf.Log("run node with args:", cf.EnvAPPHost(), cf.EnvAPPPort(), cf.EnvAPPUuid(), cf.EnvNodeUuid())
	app_id := cf.EnvAPPUuid()
	node_key := cf.EnvNodeUuid()

	ntype, subindex, ins, sess_or_app := app.getNode(node_key)
	cf.Assert(ins != nil, "Node(%s) not find", node_key)
	statops := kvops.GetKVOpImp(cf.EnvAPPIMP(), map[string]interface{}{
		"host":  cf.EnvAPPHost(),
		"port":  cf.EnvAPPPort(),
		"scope": cf.EnvAPPScope(),
	})
	runcfg_tp := cf.FrJson(cf.Base64De(statops.Get(cf.DotS(cf.K_AB_CFAPP, app_id, cf.K_MEMBER_RUNCFG)).(string))).(map[string]interface{})
	runcfg := cf.ConvertoCFG(&runcfg_tp)

	ch_cfg := cf.GetCfgC(&runcfg, "cf.services.message")
	ch_cfg["app_id"] = app_id

	msgops := chops.GetChOpsImp(ch_cfg["imp"].(string), ch_cfg)

	chs_i, chs_o, fc, ex_args := app.getInOutCh(statops, ntype, subindex, ins)
	cf.Log("start worker(", node_key, ") with:", chs_i, chs_o, fc, ex_args)

	if len(chs_i) < 1 {
		for {
			args := []interface{}{sess_or_app}
			args = append(args, ex_args...)
			rets := cf.FuncCall(fc, args)
			cf.Log(">>>>>", rets, "|", cf.AsJson(rets), "->", chs_o)
			if len(rets) > 0 {
				msgops.Put(chs_o, cf.Base64En(cf.AsJson(rets)))
			}
			time.Sleep(time.Second)
		}
	} else {
		cf.Log("watch:", chs_i)
		msgops.Watch(chs_i, func(worker, subj, data string) bool {
			args := []interface{}{sess_or_app}
			args_get := cf.ListFrJson(cf.Base64De(data))
			args = append(args, args_get...)
			args = append(args, ex_args...)
			cf.Log("<<<<<<<<<<<", subj, args, data, cf.Base64De(data))
			rets := cf.FuncCall(fc, args)
			cf.Log("|||", rets)
			return true
		})
	}
	for {
		time.Sleep(10 * time.Second)
		msgops.Put([]string{app_id + ".log"}, cf.FmStr("%s is alive", node_key))
	}
}

func NewApp(name string, cfg ...cf.CFG) *App {
	app_uid := cf.EnvAPPUuid()
	app_uid = cf.If(app_uid != "", app_uid, cf.AsMd5(cf.TimestampStr())).(string)
	return &App{
		Name:  name,
		Uuid:  app_uid,
		CTime: cf.Timestamp(),
		Svrs:  []*Service{},
		Sess:  []*Session{},
		cfg:   cf.MergeCFG(cfg...),
	}
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
	prefix := "cfapp." + app.Uuid
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
	//Log("launch:", cmd.String())
	cf.Log("*************cloudflow output*************")
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err := cmd.Start()
	cf.Assert(err == nil, "launch cf fail: %s", err)
	cf.Log(cmd.String())
	cmd.Wait()
	cf.Log("Exit App")
}

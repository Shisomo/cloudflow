package cloudflow

import (
	"cloudflow/internal/task"
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	"cloudflow/sdk/golang/cloudflow/chops"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"container/list"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
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

func (app *App) getInOutCh(kv kvops.KVOp, ntype string, subindex int, ins interface{}) ([]string, []string, int, interface{}, []interface{}) {
	chs_i := []string{}
	chs_o := []string{}
	sub_c := 0
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
		for _, n := range node.NexNodes {
			sub_c += int(kv.Get(cf.DotS(cf.K_AB_NODE, n.Uuid, cf.K_MEMBER_INSCOUNT)).(float64))
		}
		chs_o = append(chs_o, cf.DotS("out", node.Uuid, cf.Astr(subindex)))
		fc = node.Func
		ex_args = node.ExArgs
	case cf.K_AB_SERVICE:
		srv := ins.(*Service)
		chs_i = append(chs_i, cf.DotS("in", srv.Uuid, cf.Astr(subindex)))
		chs_o = append(chs_o, cf.DotS("out", srv.Uuid, cf.Astr(subindex)))
		fc = srv.Func
		ex_args = srv.ExArgs
	default:
		cf.Assert(false, "%s not supported", ntype)
	}
	return chs_i, chs_o, sub_c, fc, ex_args
}

func (app *App) getNode(key string) (string, int, interface{}) {
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

func (app *App) IsExit(ntype string, node_srv interface{}) bool {
	switch ntype {
	case cf.K_AB_SERVICE:
		return node_srv.(*Service).IsExit
	case cf.K_AB_NODE:
		return node_srv.(*Node).IsExit
	default:
		cf.Assert(false, "%s not supported", ntype)
	}
	return false
}

func (app *App) runNode() {
	cf.LogSetPrefix("<" + cf.EnvNodeUuid() + "> ")

	cf.Log("run node with args:", cf.EnvAPPHost(), cf.EnvAPPPort(), cf.EnvAPPUuid(), cf.EnvNodeUuid())
	app_id := cf.EnvAPPUuid()
	node_key := cf.EnvNodeUuid()

	ntype, subindex, ins := app.getNode(node_key)
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
	CheckNodeSrvInsCount(statops, ntype, node_key, subindex, ins)

	chs_i, chs_o, subs_count, fc, ex_args := app.getInOutCh(statops, ntype, subindex, ins)
	cf.Log("start worker(", node_key, ") with:", chs_i, chs_o, fc, ex_args)

	working := true
	msg_index := 1
	empty_ret := cf.FuncEmptyRet(fc)
	if len(chs_i) < 1 {
		// data source
		for {
			args := []interface{}{ins}
			args = append(args, ex_args...)
			rets := cf.FuncCall(fc, args)
			if app.IsExit(ntype, ins) {
				for i := 0; i < 2*subs_count; i++ {
					// Tell sub-nodes to exit
					msgops.Put(chs_o, cf.MakeMsg(msg_index, empty_ret, cf.K_MESSAGE_EXIT))
				}
				working = false
				break
			}
			cf.Assert(len(rets) > 0, "func ret empty")
			msgops.Put(chs_o, cf.MakeMsg(msg_index, rets, cf.K_MESSAGE_NORM))
			msg_index += 1
		}
	} else {
		// data process
		cf.Log("watch:", chs_i)
		// data cache
		syc := func(t string, v interface{}) bool {
			switch t {
			case cf.K_AB_SERVICE:
				return false
			case cf.K_AB_NODE:
				return v.(*Node).Synchz
			}
			return false
		}(ntype, ins)
		data_cache := initDataCache(chs_i, syc)
		msgops.Watch(chs_i, func(worker, subj, data string) bool {
			dmsg := cf.ParsMsg(data)
			// check Exit
			data_cache.put(subj, dmsg["ctrl_data"].(string), int(dmsg["index"].(float64)), dmsg["app_data"])
			if data_cache.exit {
				for i := 0; i < 2*subs_count; i++ {
					// Tell sub-nodes to exit
					msgops.Put(chs_o, cf.MakeMsg(msg_index, empty_ret, cf.K_MESSAGE_EXIT))
				}
				working = false
			}
			return true
		})
		// loop callback
		for {
			if !working {
				break
			}
			args := []interface{}{ins}
			args_get := data_cache.get()
			if len(args_get) < 1 {
				continue
			}
			args = append(args, args_get...)
			args = append(args, ex_args...)
			rets := cf.FuncCall(fc, args)
			if len(rets) > 0 {
				msgops.Put(chs_o, cf.MakeMsg(0, rets, cf.K_MESSAGE_NORM))
				msg_index += 1
			}
		}
	}

	// update task state
	parent := statops.Get(cf.DotS(node_key, cf.K_MEMBER_PARENT)).(string)
	listky := statops.Get(cf.DotS(parent,
		cf.If(ntype == cf.K_AB_SERVICE, cf.K_AB_SERVICE, cf.K_AB_NODE).(string))).(string)
	tsk := task.Task{
		List_key: listky,
		Uuid_key: node_key,
	}
	task.UpdateStat(statops, tsk, cf.K_STAT_EXIT, node_key)
	msgops.Put([]string{app_id + ".log"}, cf.FmStr("%s exit", node_key))
}

func CheckNodeSrvInsCount(ops kvops.KVOp, ntype string, node_key string, subindex int, ins interface{}) {
	instance_count := int(cfmodule.GetVal(ops, node_key, cf.K_MEMBER_INSCOUNT).(float64))
	switch ntype {
	case cf.K_AB_SERVICE:
		ins_count := ins.(*Service).InsCount
		if ins_count > 0 {
			cf.Assert(ins_count == instance_count, "error instance count not persistent %d != %d", ins_count, instance_count)
		} else {
			ins.(*Service).InsCount = instance_count
		}
		ins.(*Service).SubIdx = subindex
	case cf.K_AB_NODE:
		ins_count := ins.(*Node).InsCount
		if ins_count > 0 {
			cf.Assert(ins_count == instance_count, "error instance count not persistent %d != %d", ins_count, instance_count)
		} else {
			ins.(*Node).InsCount = instance_count
		}
		ins.(*Node).SubIdx = subindex
	default:
		cf.Assert(false, "%s not support", ntype)
	}
}

type DataCache struct {
	lock       sync.Mutex
	sync       bool
	exit       bool
	dach_chans []string
	dach_cache map[string]*list.List
	dach_stats map[string]string
	uuid_chans map[string][]string
	uuid_names []string
	exit_chans []string
}

func initDataCache(chs []string, sync bool) *DataCache {
	dc := DataCache{
		sync:       sync,
		exit:       false,
		dach_chans: chs,
		dach_cache: map[string]*list.List{},
		dach_stats: map[string]string{},
		uuid_chans: map[string][]string{},
		uuid_names: []string{},
		exit_chans: []string{},
	}
	for _, v := range chs {
		vlist := strings.Split(v, ".")
		uuid := strings.Join(vlist[:2], ".")
		if !cf.StrListHas(dc.uuid_names, uuid) {
			dc.uuid_names = append(dc.uuid_names, uuid)
		}
		cf.Assert(!cf.StrListHas(dc.uuid_chans[uuid], v), "chanel repeated! :%s", chs)
		dc.uuid_chans[uuid] = append(dc.uuid_chans[uuid], v)
		dc.dach_cache[v] = list.New()
	}
	return &dc
}

// FIXME: Here requires Congestion Control
func (dc *DataCache) put(ch string, cdata string, t_index int, t_data interface{}) {
	dc.lock.Lock()
	value := map[string]interface{}{
		"index": t_index,
		"data":  t_data,
	}
	dc.dach_cache[ch].PushBack(value)
	if dc.dach_stats[ch] != cf.K_MESSAGE_EXIT {
		dc.dach_stats[ch] = cdata
		// add to exit chanes
		if cdata == cf.K_MESSAGE_EXIT {
			dc.exit_chans = append(dc.exit_chans, ch)
		}
	}
	if len(dc.exit_chans) == len(dc.dach_chans) {
		dc.exit = true
	}
	dc.lock.Unlock()
}

func (dc *DataCache) get() []interface{} {
	emp := []interface{}{}
	ret := []interface{}{}
	ids := [][]int{}
	for index, uuid := range dc.uuid_names {
		ids = append(ids, []int{})
		data := [][]interface{}{}
		batch_size := 0
		batch_need := len(dc.uuid_chans[uuid])
		for _, ch := range dc.uuid_chans[uuid] {
			// in sync mode, need all channes data
			dc.lock.Lock()
			elem := dc.dach_cache[ch].Front()
			if elem == nil {
				dc.lock.Unlock()
				if dc.sync {
					break
				} else {
					continue
				}
			}

			value := elem.Value.(map[string]interface{})
			if dc.dach_stats[ch] != cf.K_MESSAGE_EXIT {
				dc.dach_cache[ch].Remove(elem)
			}
			dc.lock.Unlock()

			// set index
			id := value["index"].(int)
			ids[index] = append(ids[index], id)
			// set data
			data = cf.ZipAppend(data, value["data"].([]interface{}))
			// del from cache
			batch_size += 1
		}
		if dc.sync {
			if batch_size < batch_need {
				//	for _, ch := range dc.uuid_chans[uuid] {
				//		cf.Log("-->", ch, dc.dach_cache[ch].Len())
				//}
				//cf.Log("")
				return emp
			}
		}
		if len(data) < 1 {
			return emp
		}
		ret = append(ret, cf.SzOneDim(data, len(dc.uuid_chans[uuid]) == 1)...)
	}
	if dc.sync {
		v := ids[0][0]
		for _, l := range ids {
			for _, a := range l {
				cf.Assert(a == v, "index not the same in sync mode: %s", cf.Astr(ids))
			}
		}
	}
	return ret
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

package cloudflow

import (
	"encoding/json"
)


type App struct {
	Name  string      `json:"name"`
	Uuid  string      `json:"uuid"`
	Svrs  []*Service  `json:"srvs"`
	Sess  []*Session  `json:"sess"`
	CTime int64       `json:"ctime"`
}


func (app *App) CreateSession(name string) *Session {
	return NewSession(app, name)
}


func (app *App )Reg(fc interface{}, name string, kwargs... interface{}) *App{
	NewService(app, fc, name, kwargs)
	return app
}


func NewApp(name string) *App{
	app_uid := EnvAPPUuid()
	app_uid = If(app_uid != "", app_uid, AsMd5(TimestampStr())).(string)
	return &App{
		Name: name,
		Uuid: app_uid,
		CTime: Timestamp(),
		Svrs: []*Service{},
		Sess: []*Session{},
	}
}


func (app *App)CompareJson(jdata string) bool {
	new_app := App{}
	if err := json.Unmarshal([]byte(jdata), &new_app); err != nil {
		return false
	}
	for sdx, sess := range new_app.Sess{
		uuid_sess := app.Sess[sdx].Uuid
		Assert(sess.Uuid == uuid_sess, "Check Session[%s] fail: session:%d != %s", sess.Uuid, sdx, uuid_sess)
		for fdx, flow := range sess.Flows{
			uuid_flow := app.Sess[sdx].Flows[fdx].Uuid
			Assert(flow.Uuid == uuid_flow, "Check flow[%s] fail: session:%d-flow:%d != %s", flow.Uuid, sdx, fdx, uuid_flow)
			for ndx, node := range flow.Nodes{
				uuid_node := app.Sess[sdx].Flows[fdx].Nodes[ndx].Uuid
				Assert(node.Uuid == uuid_node, 
					   "check Node(%s) fail: session:%d-flow:%d-Node:%d != %s", node.Uuid, sdx, fdx, ndx, uuid_node)
			}
		}
	}
	return true
}


func (app *App)ExportJson() string{
	enc, err := json.MarshalIndent(app, "", " ")
	if err != nil {
		Err(err)
	}
	jdata := string(enc)
	Assert(app.CompareJson(jdata), "export compare fail!")
	return jdata
}


func (app *App)ExportConfigJson() (string, string){
	var appdata map[string]interface{}
	cfgjson := app.ExportJson()
	json.Unmarshal([]byte(cfgjson), &appdata)

	var exportJS = make(map[string]interface{})
	prefix := CfgKeyPrefix() + app.Uuid
	exportJS[prefix+".rawcfg"] = Base64En(cfgjson)
	exportJS[prefix+".sdkv"] = Version()
	DumpKV(&appdata, &exportJS, prefix, "uuid")

	return prefix, AsJson(&exportJS)
}


func (app *App) Run(){
}

package cloudflow

import (
	"cloudflow/sdk/golang/cloudflow/chops"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"cloudflow/sdk/golang/cloudflow/task"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

type Service struct {
	IsExit    bool            `json:"-"`
	Name      string          `json:"name"`
	App       *App            `json:"-"`
	Func      interface{}     `json:"-"`
	Uuid      string          `json:"uuid"`
	Idx       int             `json:"index"`
	SubIdx    int             `json:"subidx"`
	ExArgs    []interface{}   `json:"-"`
	InsCount  int             `json:"inscount"`
	UserData  interface{}     `json:"-"`
	kvOps     kvops.KVOp      `json:"-"`
	chOps     chops.ChannelOp `json:"-"`
	callCount int64           `json:"-"`
	cf.CommStat
}

func (srv *Service) MarshalJSON() ([]byte, error) {
	type JService Service
	func_name := strings.Replace(reflect.ValueOf(srv.Func).String(), "func(",
		runtime.FuncForPC(reflect.ValueOf(srv.Func).Pointer()).Name()+"(", 1)
	return json.Marshal(&struct {
		*JService
		Func string `json:"func"`
	}{
		JService: (*JService)(srv),
		Func:     func_name,
	})
}

var __srv_index__ int = 0

func NewService(app *App, fc interface{}, name string, args ...interface{}) *Service {
	ex_args, options := ParsOptions(args...)
	var srv = Service{
		IsExit:   false,
		Name:     name,
		App:      app,
		Func:     fc,
		Idx:      __srv_index__,
		SubIdx:   0,
		ExArgs:   ex_args,
		InsCount: 1,
		UserData: nil,
		kvOps:    nil,
	}
	srv.CTime = cf.Timestamp()
	cf.UpdateObject(&srv, options)
	srv.Parent = cf.DotS(cf.K_AB_CFAPP, app.Uuid)
	srv.Cstat = cf.K_STAT_WAIT
	srv.AppUid = app.Uuid
	srv.IsExit = false
	app.Svrs = append(app.Svrs, &srv)
	srv.UpdateUuid()
	__srv_index__ += 1
	return &srv
}

func (srv *Service) String() string {
	return fmt.Sprintf("Service(%s, %s)", srv.Uuid, srv.Name)
}

func (srv *Service) UpdateUuid() {
	srv.Uuid = cf.AsMd5(srv.App.Uuid + ".services." + cf.Itos(srv.Idx) + "." + cf.Itos(srv.SubIdx))
}

// RunInterface
func (srv *Service) Exited() bool {
	return srv.IsExit
}

func (srv *Service) StartCall() {
	srv.callCount = 0
}

func (srv *Service) PreCall() {
	srv.callCount += 1
}

func (srv *Service) Call(a []interface{}) []interface{} {
	args := []interface{}{srv}
	args = append(args, a...)
	args = append(args, srv.ExArgs...)
	srv.PreCall()
	ret := cf.FuncCall(srv.Func, args)
	if srv.Exited() {
		srv.callCount -= 1
	}
	return ret
}

func (srv *Service) SyncState() {
	// TBD
}

func (srv *Service) NeedExit() bool {
	return false
}

func (srv *Service) Exit(reason string) {
	srv.IsExit = true
	srv.ExitLog = reason
}

func (srv *Service) InOutChs() ([]string, []string) {
	return []string{cf.DotS(srv.Uuid, "int")}, []string{cf.DotS(srv.Uuid, "out")}
}

func (srv *Service) SetSubIdx(idx int) {
	srv.SubIdx = idx
}

func (srv *Service) SetKVOps(ops kvops.KVOp) {
	srv.kvOps = ops
}

func (srv *Service) FuncName() string {
	return cf.FuncName(srv.Func)
}

func (srv *Service) InstanceCount() int {
	return srv.InsCount
}

func (srv *Service) GetBatchSize() int {
	return 1
}

func (srv *Service) GetExitChs() (map[string][]interface{}, bool) {
	return map[string][]interface{}{}, false
}

func (srv *Service) UpdateUUID(node_key string) {
	srv.Uuid = strings.Replace(node_key, cf.K_AB_SERVICE+".", "", 1)
}

func (srv *Service) SetMsgOps(ops chops.ChannelOp) {
	srv.chOps = ops
}

func (srv *Service) msg(txt string) {
	cf.Log(txt)
	srv.chOps.Put([]string{srv.AppUid + ".log"}, txt)
}

func (srv *Service) MsgLog(a ...interface{}) {
	srv.msg(fmt.Sprint(a...))
}

func (srv *Service) MsgLogf(fmt string, a ...interface{}) {
	srv.msg(cf.FmStr(fmt, a...))
}

func (srv *Service) UUID() string {
	return srv.Uuid
}

func (srv *Service) IsIgnoreRet() bool {
	return false
}

func (srv *Service) IgnoreRet() {
}

func (srv *Service) CallCount() int64 {
	return srv.callCount
}

func (srv *Service) AsTask() task.Task {
	return task.Task{
		List_key: cf.DotS(srv.Parent, cf.K_AB_NODE),
		Uuid_key: cf.DotS(cf.K_AB_NODE, srv.Uuid),
	}
}

func (srv *Service) PerfLogInter() int {
	return 0
}

func (srv *Service) GetName() string {
	return srv.Name
}

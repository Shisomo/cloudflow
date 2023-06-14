package cloudflow

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

type Service struct {
	Name     string        `json:"name"`
	App      *App          `json:"-"`
	Func     interface{}   `json:"-"`
	Uuid     string        `json:"uuid"`
	Idx      int           `json:"index"`
	SubIdx   int           `json:"subidx"`
	ExArgs   []interface{} `json:"-"`
	InsCount int           `json:"inscount"`
	CTime    int64         `json:"ctime"`
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

func NewService(app *App, fc interface{}, name string, ex_args ...interface{}) *Service {
	var srv = Service{
		Name:     name,
		App:      app,
		Func:     fc,
		Idx:      __srv_index__,
		SubIdx:   0,
		ExArgs:   ex_args,
		InsCount: 1,
		CTime:    cf.Timestamp(),
	}
	srv.Parent = "cfapp." + app.Uuid
	srv.Cstat = cf.K_STAT_WAIT
	srv.AppUid = app.Uuid
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

func (svr *Service) call(arg ...interface{}) interface{} {
	// TBD
	return 1
}

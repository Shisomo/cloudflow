package cloudflow

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
)

type Session struct {
	Name       string  `json:"name"`
	Uuid       string  `json:"uuid"`
	App        *App    `json:"-"`
	Flows      []*Flow `json:"flow"`
	Idx        int     `json:"index"`
	storageOps StorageOps
	cf.CommStat
}

var __session_index__ int = 0

func NewSession(app *App, name string) *Session {
	ses := Session{
		Name:       name,
		Uuid:       cf.AsMd5(app.Uuid + cf.Itos(__session_index__)),
		App:        app,
		Idx:        __session_index__,
		Flows:      []*Flow{},
		storageOps: nil,
	}
	ses.CTime = cf.Timestamp()
	ses.Parent = "cfapp." + app.Uuid
	app.Sess = append(app.Sess, &ses)
	__session_index__ += 1
	return &ses
}

func (se *Session) CreateFlow(name string) *Flow {
	return NewFlow(se, name)
}

func (se *Session) GetStorageOps() StorageOps {
	return se.storageOps
}

package cloudflow

type Session struct {
	Name  string         `json:"name"`
	Uuid  string         `json:"uuid"`
	App   *App           `json:"-"`
	Flows []*Flow        `json:"flows"`
	Idx   int            `json:"index"`
	CTime int64          `json:"ctime"`
}

var __session_index__ int = 0
func NewSession(app *App, name string) *Session {
	ses := Session{
		Name: name,
		Uuid: AsMd5(app.Uuid + Itos(__session_index__)),
		App:  app,
		Idx: __session_index__,
		CTime: Timestamp(),
		Flows: []*Flow{},
	}
	app.Sess = append(app.Sess, &ses)
	__session_index__ += 1
	return &ses
}

func (se *Session) CreateFlow(name string) *Flow{
	return NewFlow(se, name)
}

package cloudflow

type Session struct {
	Name  string
	Uuid  string
	App   *App
	Flows []*Flow
	Idx   int
}

var __session_index__ int = 0
func NewSession(app *App, name string) *Session {
	ses := Session{
		Name: name,
		Uuid: AsMd5(app.Uuid + Itos(__session_index__)),
		App:  app,
		Idx: __session_index__,
	}
	__session_index__ += 1
	return &ses
}

func (se *Session) CreateFlow(name string) *Flow{
	return NewFlow(se, name)
}

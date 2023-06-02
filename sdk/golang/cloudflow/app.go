package cloudflow


type App struct {
	Name string
	Uuid string
	Sess []*Session
}


func (app *App) CreateSession(name string) *Session {
	return NewSession(app, name)
}


func NewApp(name string) *App{
	return &App{
		Name: name,
		Uuid: AsMd5(TimestampStr()),
	}
}


func (app *App) Run(){
}

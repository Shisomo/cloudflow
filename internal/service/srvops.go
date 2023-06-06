package service

type ServiceOps interface {
	Start()   bool
	Stop()    bool
	Restart() bool
	Started() bool
	Kill()    bool
}

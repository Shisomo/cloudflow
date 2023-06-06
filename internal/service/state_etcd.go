package service

import (
	"os/exec"
	cf "cloudflow/sdk/golang/cloudflow"
)

type StateEtcd struct {
	EctdHost string
	EctdPort int
	AppScope string
	cmd      *exec.Cmd
}

func NewStateEtcd(cfg map[string]interface{}) *StateEtcd{
	return & StateEtcd {
		EctdHost: cfg["host"].(string),
		EctdPort: cfg["port"].(int),
		AppScope: cfg["scope"].(string),
		cmd: nil,
	}
}

func (se *StateEtcd) Start() bool{
	se.cmd = exec.Command("etcd", "--data-dir /tmp")
	err := se.cmd.Start()
	cf.Assert(err==nil, "start etcd error: %s", err)
	cf.Log("start etcd with Pid:", se.cmd.Process.Pid)
	return true
}

func (se *StateEtcd) Kill() bool{
	return false
}

func (se *StateEtcd) Stop() bool{
	return false
}

func (se *StateEtcd) Restart() bool{
	se.Kill()
	return se.Start()
}

func (se *StateEtcd) Started() bool{
	return false
}

func (se *StateEtcd) ClearAll(){
}

func (se *StateEtcd) Get(key string) interface{} {
	return nil
}

func (se *StateEtcd) Set(key string) bool {
	return false
}

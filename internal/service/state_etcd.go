package service

import (
	"bytes"
	cf "cloudflow/sdk/golang/cloudflow"
	kv "cloudflow/sdk/golang/cloudflow/kvops"
	"os/exec"
	"strings"
)

type StateEtcd struct {
	EtcdHost string
	EtcdPort int
	EtcdUrls []string
	isLocal  bool
	EtcdOps  *kv.EtcDOps
	cmd      *exec.Cmd
	stderr   bytes.Buffer
	stdout   bytes.Buffer
	AppScope string
}

func NewStateEtcd(cfg map[string]interface{}) *StateEtcd {
	host := cfg["host"].(string)
	port := cfg["port"].(int)
	ports := cf.Itos(port)
	local := host == "localhost" || host == "127.0.0.1"
	cnn_urls := []string{}
	if local {
		cnn_urls = append(cnn_urls, "http://"+host+":"+ports)
	} else {
		for _, url := range strings.Split(host, ";") {
			url = strings.TrimSpace(url)
			if !strings.Contains(url, ":") {
				url = url + ":" + ports
			}
			if !strings.Contains(url, "http") {
				url = "http://" + url
			}
			cnn_urls = append(cnn_urls, url)
		}
	}
	return &StateEtcd{
		EtcdHost: host,
		EtcdPort: port,
		isLocal:  local,
		EtcdUrls: cnn_urls,
		EtcdOps:  nil,
		AppScope: cfg["scope"].(string),
	}
}

func (se *StateEtcd) Start() bool {
	if se.isLocal {
		pid := cf.ProcessPID("etcd")
		if pid < 0 {
			_, err := exec.LookPath("etcd")
			cf.Assert(err == nil, "etcd not found, make sure it is installed")
			se.cmd = exec.Command("etcd", "--data-dir", "/tmp", "--listen-client-urls", se.EtcdUrls[0],
				"--advertise-client-urls", se.EtcdUrls[0])
			se.cmd.Stderr = &se.stderr
			se.cmd.Stdout = &se.stdout
			err = se.cmd.Start()
			cf.Assert(err == nil, "start etcd error: %s \nstderr:\n%s\nstdout:\n%s", err, se.stderr.String(), se.stdout.String())
			cf.Log("start etcd with Pid:", se.cmd.Process.Pid)
		} else {
			cf.Log("etcd is already on localhost:", pid)
		}
	} else {
		cf.Log("ignore no local etcd startup")
	}
	cf.Log("creat client with endpoints:", se.EtcdUrls)
	se.EtcdOps = kv.NewEtcDOps(se.EtcdUrls, se.AppScope)
	check_key := "atime" + cf.AsMd5(cf.AppID()) + cf.TimestampStr()
	se.Set(check_key, check_key)
	rkey := se.Get(check_key)
	if rkey == nil {
		cf.Log("verify etcd fail:", check_key, "!=", rkey)
		return false
	}
	if rkey != check_key {
		cf.Log("verify etcd fail:", check_key, "!=", rkey)
		return false
	}
	se.Del(check_key)
	cf.Log("check etcd success")
	return true
}

func (se *StateEtcd) Kill() bool {
	if se.cmd != nil {
		err := se.cmd.Process.Kill()
		cf.Assert(err == nil, "Kill etcd process fail: %s", err)
	}
	se.cmd = nil
	return true
}

func (se *StateEtcd) Stop() bool {
	return se.Kill()
}

func (se *StateEtcd) Restart() bool {
	se.Kill()
	return se.Start()
}

func (se *StateEtcd) Started() bool {
	return se.cmd != nil
}

func (se *StateEtcd) Get(key string) interface{} {
	return se.EtcdOps.Get(key)
}

func (se *StateEtcd) Del(key string) bool {
	return se.EtcdOps.Del(key)
}

func (se *StateEtcd) Set(key string, val interface{}) bool {
	return se.EtcdOps.Set(key, val)
}

func (se *StateEtcd) SetKV(Kv map[string]interface{}, ignore_empty bool) bool {
	return se.EtcdOps.SetKV(Kv, ignore_empty)
}

func (se *StateEtcd) GetKs(Kv []string, ignore_empty bool) map[string]interface{} {
	return se.EtcdOps.GetKs(Kv, ignore_empty)
}

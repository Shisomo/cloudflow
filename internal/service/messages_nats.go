package service

import (
	"bytes"
	"cloudflow/sdk/golang/cloudflow/chops"
	ch "cloudflow/sdk/golang/cloudflow/chops"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"os/exec"
)

type MessageNats struct {
	scope      interface{}
	host       string
	port       int
	isLocal    bool
	cmd        *exec.Cmd
	stderr     bytes.Buffer
	stdout     bytes.Buffer
	ChannelOps ch.ChannelOp
}

func NewMessageNats(cfg map[string]interface{}) *MessageNats {
	cf.Log("creat nats message")
	host := cfg["host"].(string)
	port := cfg["port"].(int)
	scop := cfg["app_id"]
	local := host == "localhost" || host == "127.0.0.1"
	mnats := MessageNats{
		host:       host,
		port:       port,
		isLocal:    local,
		cmd:        nil,
		ChannelOps: nil,
		scope:      scop,
	}
	return &mnats
}

func (msg *MessageNats) Start() bool {
	if msg.isLocal {
		pid := cf.ProcessPID("nats-server")
		if pid < 0 {
			msg.cmd = exec.Command("nats-server", "--js", "-a", msg.host, "-p", cf.Itos(msg.port))
			msg.cmd.Stderr = &msg.stderr
			msg.cmd.Stdout = &msg.stdout
			cf.Log("run:", msg.cmd.String())
			err := msg.cmd.Start()
			cf.Assert(err == nil, "start nats-server error: %s \nstderr:\n%s\nstdout:\n%s", err, msg.stderr.String(), msg.stdout.String())
			cf.Log("start nats-server with Pid:", msg.cmd.Process.Pid)
		} else {
			cf.Log("nats-server is already on: ", pid)
		}
	}
	// check nats-server useable
	cf.Assert(ch.CheckNats(msg.host, msg.port) == true, "test nats fail")
	cf.Log("check nat-server with jetstream success")
	if msg.scope != nil {
		msg.ChannelOps = chops.NewNatsChOp(cf.MakeNatsUrl(msg.host, msg.port), msg.scope.(string))
	}
	return true
}

func (msg *MessageNats) Stop() bool {
	return msg.Kill()
}
func (msg *MessageNats) Restart() bool {
	msg.Kill()
	return msg.Start()
}
func (msg *MessageNats) Started() bool {
	return msg != nil
}
func (msg *MessageNats) Kill() bool {
	if msg.cmd != nil {
		err := msg.cmd.Process.Kill()
		cf.Assert(err == nil, "Kill nats-server process fail: %s", err)
	}
	msg.cmd = nil
	return true
}

func (msg *MessageNats) GetChannelOps() chops.ChannelOp {
	return msg.ChannelOps
}

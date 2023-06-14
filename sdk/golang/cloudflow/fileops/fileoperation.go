package fileops

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"strings"
)

type FileOps interface {
	Put(file_name string, file_path string) bool
	Get(file_name string, file_path string) bool
	Close()
	Conn() bool
	Clear() bool
}

func GetFileOps(scope string, cfg map[string]interface{}) FileOps {
	name := cfg["imp"].(string)
	switch name {
	case "nats":
		host := cfg["host"].(string)
		port := cf.Astr(cfg["port"])
		cnn := "nats://" + host + ":" + port
		if strings.Contains(host, "/") {
			cnn = host
		}
		return NewFileOpsNats(cnn, scope)
	default:
		cf.Assert(false, "%s (FileOps) not supported", name)
	}
	return nil
}

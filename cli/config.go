package cli

import (
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os/user"
	"os"
	cf "cloudflow/sdk/golang/cloudflow"
)


var CMD_Config = &cobra.Command{
	Use:   "config",
	Short: "a short discription of config",
	Long:  "config is ...., long description",
	Run: func(cmd *cobra.Command, args []string) {
		cf.Log("run config", args)
    },
}


func LoadCfg(file string) map[string]interface{}{
	cuser, err := user.Current()
	cf.Assert(err == nil, "Get current user fail: %s", err)
	default_cfgs := []string{
		"./default.yaml",
		"./config/default.yaml",
		"./config/app/default.yaml",
		cuser.HomeDir + "/default.yaml",
		"/etc/cloudflow/default.yaml",
	}
	if file == ""{
		for _, f := range default_cfgs{
			_, r := os.Stat(f)
			cf.Log("search config file:", f)
			if r == nil {
				file = f
				break
			}
		}
	}
	cf.Assert(file != "", "config file not find:%s", file)
	data, err := ioutil.ReadFile(file)
	cf.Assert(err == nil, "Read file(%s) error:%s", file, err)
	var cfg_data map[string]interface{}
	yaml.Unmarshal(data, &cfg_data)
	return cfg_data
}


func GetAppCfg() map[string]interface{} {
	cfg := LoadCfg(Cf_file)
	// overwrite
	if Cf_host != "" {
		cf.SetCfg(&cfg, "cf.services.state.host", Cf_host)
	}
	if Cf_port != 0 {
		cf.SetCfg(&cfg, "cf.services.state.port", Cf_port)
	}
	return cfg
}

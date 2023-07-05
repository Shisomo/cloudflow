package cli

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"os"

	"github.com/spf13/cobra"
)

var Cf_host string
var Cf_file string
var Cf_skvs string
var Cf_port int64
var Cf_debug bool

var CfCmd = &cobra.Command{
	Use:       "cf <sub_commd>",
	Short:     "a short discription of cloudflow (cf)",
	Long:      "cloudflow (cf) is a ...., long description",
	ValidArgs: []string{"help"},
	Args:      cobra.ExactValidArgs(1),
	Version:   cf.Version(),
	Run: func(cmd *cobra.Command, args []string) {
		commPreProcess()
		cf.Err("Not here")
	},
}

func Main() {
	CfCmd.AddCommand(CMD_Build, CMD_Config, CMD_UI, CMD_example)
	CfCmd.AddCommand(CMD_Run, CMD_Stat, CMD_Worker, CMD_schedule)
	CfCmd.Execute()
}

func init() {
	pflag := CfCmd.PersistentFlags()
	pflag.StringVarP(&Cf_file, "cfg", "c", "", "cf configuration file")
	pflag.StringVar(&Cf_skvs, "setkv", "", "overwrite cfg file k-v, eg: --setkv cf.host=1.2.3.4")
	pflag.StringVarP(&Cf_host, "host", "H", "", "cf runtime host (etcd host)")
	pflag.Int64VarP(&Cf_port, "port", "p", 0, "cf runtime port (etcd port)")
	pflag.BoolVarP(&Cf_debug, "debug", "d", false, "output debug info")
}

func commPreProcess() {
	if os.Getenv("CF_DEBUG") == "true" && !Cf_debug {
		cf.Log("enable debug by ENV Setting")
		Cf_debug = true
	}
	if !Cf_debug {
		cf.DisableLog()
	} else {
		os.Setenv("CF_DEBUG", "true")
	}
}

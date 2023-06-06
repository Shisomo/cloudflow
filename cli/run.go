package cli

import (
	"github.com/spf13/cobra"
	it "cloudflow/internal"
	cf "cloudflow/sdk/golang/cloudflow"
)

var app_scope string
var app_nodeid string

var CMD_Run = &cobra.Command{
	Use:   "run <app_id> <app_cfg> <app_exec_file>",
	Short: "launch a cloudflow application/node",
	Long:  "run is ...., long description",
	Aliases: []string{"r"},
	Args:   cobra.ExactArgs(3),
	Run: func(cmd *cobra.Command, args []string) {
		cfg := GetAppCfg()
		cf.SetCfg(&cfg, "cf.services.state.scope", app_scope)
		cf.SetCfg(&cfg, "cf.app_nid", app_nodeid)
		it.StartService(&cfg)
		it.SubmitApp(&cfg, args[0], args[1], args[2], app_nodeid)
		it.Schedule(&cfg)
    },
}

func init(){
	pflag := CMD_Run.PersistentFlags()
	pflag.StringVarP(&app_scope, "scope",  "s", "cl_default", "cf application scope")
	pflag.StringVar(&app_nodeid, "nid", "", "node.uuid of cf application")
}

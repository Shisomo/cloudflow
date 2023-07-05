package cli

import (
	it "cloudflow/internal"
	cf "cloudflow/sdk/golang/cloudflow/comm"

	"github.com/spf13/cobra"
)

var app_scope string
var app_nodeid string

var CMD_Run = &cobra.Command{
	Use:     "run <app_id> <app_cfg> <app_exec_file> <app_args>",
	Short:   "launch a cloudflow application/node",
	Long:    "run is ...., long description",
	Aliases: []string{"r"},
	Args:    cobra.RangeArgs(3, 4),
	Run: func(cmd *cobra.Command, args []string) {
		commPreProcess()
		// init config
		cfg := GetAppCfg()
		cf.SetCfg(&cfg, "cf.services.state.scope", app_scope)
		cf.SetCfg(&cfg, "cf.app_nid", app_nodeid)
		// launch
		flow := it.NewCloudFlow(&cfg)
		flow.StartService()
		flow.StartSchAndWorker()
		app_args := ""
		if len(args) == 4 {
			app_args = args[3]
		}
		flow.SubmitApp(args[0], args[1], args[2], app_args, app_nodeid)
	},
}

func init() {
	pflag := CMD_Run.PersistentFlags()
	pflag.StringVarP(&app_scope, "scope", "s", "cl", "cf application scope")
	pflag.StringVar(&app_nodeid, "nid", "", "node.uuid of cf application")
}

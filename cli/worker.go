package cli

import (
	it "cloudflow/internal"
	"cloudflow/internal/worker"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"time"

	"github.com/spf13/cobra"
)

var CMD_Worker = &cobra.Command{
	Use:   "worker",
	Short: "a short discription of worker",
	Long:  "worker is ...., long description",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := GetAppCfg()
		cf.SetCfg(&cfg, "cf.services.state.scope", app_scope)
		cf.SetCfg(&cfg, "cf.app_nid", app_nodeid)
		ins := it.NewCloudFlow(&cfg)
		ins.Connect()
		worker.StartWorker(cf.GetCfgC(&cfg, "cf.worker"), cf.GetCfgC(&cfg, "cf.services.fstore"), ins.StatOps)
		for {
			time.Sleep(time.Second)
		}
	},
}

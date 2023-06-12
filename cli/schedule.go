package cli

import (
	it "cloudflow/internal"
	"cloudflow/internal/schedule"
	cf "cloudflow/sdk/golang/cloudflow"
	"time"

	"github.com/spf13/cobra"
)

var CMD_schedule = &cobra.Command{
	Use:     "schedule",
	Short:   "a short discription of schedule",
	Long:    "schedule is ...., long description",
	Aliases: []string{"sc", "sch", "sched"},
	Run: func(cmd *cobra.Command, args []string) {
		cfg := GetAppCfg()
		cf.SetCfg(&cfg, "cf.services.state.scope", app_scope)
		cf.SetCfg(&cfg, "cf.app_nid", app_nodeid)
		flow := it.NewCloudFlow(&cfg)
		flow.StartService()
		schedule.StartScheduler(cf.GetCfgC(&cfg, "cf.scheduler"), flow.StateSrv)
		for {
			time.Sleep(time.Second)
		}
	},
}

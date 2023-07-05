package cli

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/ui"

	"github.com/spf13/cobra"
)

var CMD_UI = &cobra.Command{
	Use:   "UI",
	Short: "a short discription of UI",
	Long:  "UI is ...., long description",
	Run: func(cmd *cobra.Command, args []string) {
		commPreProcess()
		cfg := GetAppCfg()
		cf.SetCfg(&cfg, cf.DotS(cf.CFG_KEY_SRV_STATE, "scope"), app_scope)
		ui.StartUI(&cfg)
	},
}

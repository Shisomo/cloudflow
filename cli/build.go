package cli

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"

	"github.com/spf13/cobra"
)

var CMD_Build = &cobra.Command{
	Use:     "build",
	Short:   "a short discription of build",
	Long:    "build is ...., long description",
	Aliases: []string{"b", "bd"},
	Run: func(cmd *cobra.Command, args []string) {
		commPreProcess()
		cf.Log(args)
	},
}

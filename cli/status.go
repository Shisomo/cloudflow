package cli

import (
	"github.com/spf13/cobra"
	cf "cloudflow/sdk/golang/cloudflow"
)

var CMD_Stat = &cobra.Command{
	Use:   "status",
	Short: "a short discription of status",
	Long:  "stat is ...., long description",
	Aliases: []string{"s", "stat"},
	Run: func(cmd *cobra.Command, args []string) {
		cf.Log("stat cmd", args)
    },
}

package cli

import (
	"github.com/spf13/cobra"
	cf "cloudflow/sdk/golang/cloudflow"
)

var CMD_Worker = &cobra.Command{
	Use:   "worker",
	Short: "a short discription of worker",
	Long:  "worker is ...., long description",
	Run: func(cmd *cobra.Command, args []string) {
		cf.Log("run worker", args)
    },
}


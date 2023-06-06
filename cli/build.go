package cli

import (
	"github.com/spf13/cobra"
	cf "cloudflow/sdk/golang/cloudflow"
)

var CMD_Build = &cobra.Command{
	Use:   "build",
	Short: "a short discription of build",
	Long:  "build is ...., long description",
	Aliases: []string{"b", "bd"},
	Run: func(cmd *cobra.Command, args []string) {
		cf.Log(args)
    },
}
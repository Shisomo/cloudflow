package cli

import (
	examples "cloudflow/examples/golang"
	cf "cloudflow/sdk/golang/cloudflow/comm"

	"github.com/spf13/cobra"
)

var CMD_example = &cobra.Command{
	Use:     "example",
	Short:   "a short discription of example",
	Long:    "example is ...., long description",
	Aliases: []string{"ex", "exa", "example"},
	Args:    cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		commPreProcess()
		examples := map[string]func(a ...string){
			"wordcount": examples.Main_Wordcount,
			"gigasort":  examples.Main_GigaSort,
		}
		test, has := examples[args[0]]
		if !has {
			cf.Log(args[0], " not in: ", cf.MKeys(examples))
			return
		}
		test(args...)
	},
}

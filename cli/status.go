package cli

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

var CMD_Stat = &cobra.Command{
	Use:     "status",
	Short:   "a short discription of status",
	Long:    "stat is ...., long description",
	Aliases: []string{"s", "st", "stat"},
	Run: func(cmd *cobra.Command, args []string) {
		commPreProcess()
		cfg := GetAppCfg()
		cf.SetCfg(&cfg, "cf.services.state.scope", app_scope)
		cf.SetCfg(&cfg, "cf.app_nid", app_nodeid)
		ops := kvops.GetKVOpImp(cf.GetCfgC(&cfg, "cf.services.state"))
		max_keys := 0
		kvs := []string{}
		key := "*"
		if len(args) > 0 {
			key = args[0] + "*"
		}
		all_data := ops.Get(key)
		if all_data == nil {
			return
		}
		for k, d := range all_data.(map[string]interface{}) {
			v := cf.Astr(d)
			if strings.Contains(v, "rawcfg") {
				continue
			}
			key_sz := len(k)
			if key_sz > max_keys {
				max_keys = key_sz
			}
			kvs = append(kvs, k+"|"+v)
		}
		sort.Strings(kvs)
		for _, l := range kvs {
			f := fmt.Sprintf("%%-%ds  %%s\n", max_keys+2)
			//fmt.Println(f)
			l := strings.SplitN(l, "|", 2)
			fmt.Printf(f, l[0], l[1])
		}
	},
}

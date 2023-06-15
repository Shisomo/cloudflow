package main

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"cloudflow/sdk/golang/cloudflow/comm"
)

func statistics(app *cf.App) map[string]int {
	return map[string]int{
		"words": 1,
	}
}

func readwords(self *cf.Node, count int) int {
	comm.Log("readwords")
	return 1
}

func countwords(self *cf.Node, i int) int {
	comm.Log("countwords")
	return i + 1
}

func reducewords(se *cf.Node, st []int) {
	a := 0
	for _, v := range st {
		a += v
	}
	comm.Log("reduce>>>", a)
}

func main() {
	comm.LogSetPrefix("test-word-count ")
	comm.Log("Version", comm.Version())
	var app = cf.NewApp("test-app")
	var ses = app.CreateSession("session-1")
	var flw = ses.CreateFlow("flow-1")
	app.Reg(statistics, "record the process")
	flw.Add(readwords, "read", int(1e8)).Map(countwords, "count", 10).Reduce(reducewords, "reduce")
	app.Run()
}

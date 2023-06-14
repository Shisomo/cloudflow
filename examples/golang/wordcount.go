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

func readwords(se *cf.Session, count int64) string {
	return "123123123"
}

func countwords(se *cf.Session, txt string) map[string]int {
	return map[string]int{
		"1": 100,
	}
}

func reducewords(se *cf.Session, st map[string]int) map[string]int {
	return st
}

func main() {
	comm.LogSetPrefix("test-word-count ")
	comm.Log("Version", comm.Version())
	var app = cf.NewApp("test-app")
	var ses = app.CreateSession("session-1")
	var flw = ses.CreateFlow("flow-1")
	app.Reg(statistics, "record the process")
	flw.Add(readwords, "read", int64(1e8)).Map(countwords, "count", 10).Reduce(reducewords, "reduce")
	app.Run()
}


package main

import (
	cf "../sdk/golang/cloudflow"
)

func readwords(se *cf.Session, readfile string) string {
	return "123123123"
}

func countwords(se *cf.Session, txt string) map[string]int {
	return map[string]int {
		"1": 100,
	}
}

func reducewords(se *cf.Session, st map[string]int) map[string]int{
	return st
}

func main(){
	cf.Log("Version", cf.Version())
	var app = cf.NewApp("test-app")
	var ses = app.CreateSession("session-1")
	var flw = ses.CreateFlow("flow-1")
	flw.Add(readwords, "read", "./test.txt").Map(countwords, "count", 10).Reduce(reducewords, "reduce")
	cf.Log(flw.DrawTxt())
	app.Run()
}

package ui

import (
	"cloudflow/internal"
	"cloudflow/restful"
	cf "cloudflow/sdk/golang/cloudflow/comm"

	"github.com/gin-gonic/gin"
)

func StartUI(cfg *cf.CFG) {
	cloudflow := internal.NewCloudFlow(cfg)
	rest_app := restful.NewAppRest(cloudflow)
	r := gin.Default()
	clouflow := r.Group("/cloudflow")
	{
		app := clouflow.Group("/app")
		app.GET("", rest_app.List)
	}
	r.Run(":8888")
}

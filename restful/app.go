package restful

import (
	"cloudflow/internal"

	"github.com/gin-gonic/gin"
)

type AppRest struct {
	CloudFlow *internal.CloudFlow
}

func NewAppRest(cflow *internal.CloudFlow) *AppRest {
	return &AppRest{
		CloudFlow: cflow,
	}
}

func (self *AppRest) List(ctx *gin.Context) {
}

package cloudflow

import (
	"log"
)

type Logger struct {
}

func Log(args...interface{}){
	log.Println(args...)
}

func Err(args...interface{}){
	log.Println(args...)
}

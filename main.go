package main

import (
	"fraisedb/api/http"
	"fraisedb/base"
	"fraisedb/service"
)

func main() {
	base.InitConfig()
	base.InitLog()
	service.StartNode()
	http.InitRouter()
}

package main

import (
	"fraisedb/api/http/v2"
	"fraisedb/base"
	"fraisedb/service"
)

func main() {
	base.InitConfig()
	base.InitLog()
	service.StartNode()
	http_v2.InitRouter()
}

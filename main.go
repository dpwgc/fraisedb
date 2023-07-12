package main

import (
	"FraiseDB/api/http"
	"FraiseDB/base"
	"FraiseDB/service"
)

func main() {
	base.InitConfig()
	base.InitLog()
	service.StartNode()
	http.InitRouter()
}

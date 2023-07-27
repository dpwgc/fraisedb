package http_v2

import (
	"fmt"
	"fraisedb/base"
	"github.com/julienschmidt/httprouter"
	"net/http"
)

// InitRouter 初始化HTTP路由
func InitRouter() {

	port := fmt.Sprintf(":%v", base.Config().Node.HttpPort)

	r := httprouter.New()

	r.GET("/v2/health", getHealth)
	r.GET("/v2/config", getConfig)

	r.POST("/v2/node", addNode)
	r.DELETE("/v2/node/:endpoint", removeNode)
	r.GET("/v2/nodes", listNode)
	r.GET("/v2/leader", getLeader)

	r.POST("/v2/namespace/:namespace", createNamespace)
	r.GET("/v2/namespaces", listNamespace)
	r.DELETE("/v2/namespace/:namespace", deleteNamespace)

	r.PUT("/v2/kv/:namespace/:key", putKV)
	r.GET("/v2/kv/:namespace/:key", getKV)
	r.DELETE("/v2/kv/:namespace/:key", deleteKV)
	r.GET("/v2/kvs/:namespace/:keyPrefix", listKV)

	r.GET("/v2/subscribe/:namespace/:keyPrefix/:clientId", subscribe)

	initConsumer()
	err := http.ListenAndServe(port, r)
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
		panic(err)
	}
}

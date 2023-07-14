package http

import (
	"fmt"
	"fraisedb/base"
	"fraisedb/service"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"strconv"
	"time"
)

// InitRouter 初始化HTTP路由
func InitRouter() {

	port := fmt.Sprintf(":%v", base.Config().Node.HttpPort)

	r := httprouter.New()

	r.POST("/node", addNode)
	r.GET("/leader", getLeader)

	r.POST("/namespace/:namespace", createNamespace)
	r.GET("/namespaces", listNamespace)
	r.DELETE("/namespace/:namespace", deleteNamespace)

	r.PUT("/kv/:namespace/:key", putKV)
	r.GET("/kv/:namespace/:key", getKV)
	r.DELETE("/kv/:namespace/:key", deleteKV)
	r.GET("/kvs/:namespace/:keyPrefix", listKV)

	r.GET("/subscribe/:namespace/:keyPrefix", subscribe)

	initConsumer()
	err := http.ListenAndServe(port, r)
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
		panic(err)
	}
}

func addNode(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	command, err := readNodeCommand(r)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	err = service.AddNode(command.Addr, command.Port)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	result(w, true, nil, "")
}

func getLeader(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	leader := service.GetLeader()
	result(w, true, leader, "")
}

func listNamespace(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	ns := service.ListNamespace()
	result(w, true, ns, "")
}

func createNamespace(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	namespace := p.ByName("namespace")
	err := service.CreateNamespace(namespace)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	result(w, true, nil, "")
}

func deleteNamespace(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	namespace := p.ByName("namespace")
	err := service.DeleteNamespace(namespace)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	result(w, true, nil, "")
}

func putKV(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	namespace := p.ByName("namespace")
	key := p.ByName("key")
	command, err := readKVCommand(r)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	var ddl int64 = 0
	if command.TTL > 0 {
		ddl = time.Now().Unix() + command.TTL
	}
	err = service.PutKV(namespace, key, command.Value, ddl)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	result(w, true, ddl, "")
}

func getKV(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	namespace := p.ByName("namespace")
	key := p.ByName("key")
	value, err := service.GetKV(namespace, key)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	result(w, true, value, "")
}

func deleteKV(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	namespace := p.ByName("namespace")
	key := p.ByName("key")
	err := service.DeleteKV(namespace, key)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	result(w, true, nil, "")
}

func listKV(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	namespace := p.ByName("namespace")
	keyPrefix := p.ByName("keyPrefix")
	limitStr := r.URL.Query().Get("limit")
	limit, err := strconv.ParseInt(limitStr, 10, 64)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	kvs, err := service.ListKV(namespace, keyPrefix, limit)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	result(w, true, kvs, "")
}

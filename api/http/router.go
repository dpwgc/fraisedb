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
	r.PUT("/kv", putKV)
	r.GET("/kv/:key", getKV)
	r.DELETE("/kv/:key", delKV)
	r.GET("/kvs", listKV)
	r.GET("/kvs/subscribe/:keyPrefix/:connId", subscribe)

	initEventConsumer()
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

func putKV(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	command, err := readKVCommand(r)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	var ddl int64 = 0
	if command.TTL > 0 {
		ddl = time.Now().Unix() + command.TTL
	}
	err = service.PutKV(command.Key, command.Value, ddl)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	result(w, true, ddl, "")
}

func getKV(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	key := p.ByName("key")
	value, err := service.GetKV(key)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	result(w, true, value, "")
}

func delKV(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	key := p.ByName("key")
	err := service.DeleteKV(key)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	result(w, true, nil, "")
}

func listKV(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	keyPrefix := r.URL.Query().Get("keyPrefix")
	limitStr := r.URL.Query().Get("limit")
	limit, err := strconv.ParseInt(limitStr, 10, 64)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	kvs, err := service.ListKV(keyPrefix, limit)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	result(w, true, kvs, "")
}

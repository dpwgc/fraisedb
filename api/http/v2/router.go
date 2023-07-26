package http_v2

import (
	"encoding/json"
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

	r.GET("/v2/subscribe/:namespace/:keyPrefix", subscribe)

	initConsumer()
	err := http.ListenAndServe(port, r)
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
		panic(err)
	}
}

func getHealth(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	_, err := w.Write([]byte("1"))
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
	}
}

func getConfig(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	marshal, err := json.Marshal(base.Config())
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	_, err = w.Write(marshal)
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
	}
}

func addNode(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	command, err := readNodeCommand(r)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}

	endpoint := fmt.Sprintf("%s:%v", command.Addr, command.HttpPort)

	// 节点健康检查
	healthRes, err := base.HttpGet(fmt.Sprintf("http://%s/v2/health", endpoint))
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	if healthRes == nil || string(healthRes) != "1" {
		result(w, false, nil, "the node is unhealthy")
		return
	}

	// 节点配置检查
	configRes, err := base.HttpGet(fmt.Sprintf("http://%s/v2/config", endpoint))
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	if configRes == nil || len(configRes) == 0 {
		result(w, false, nil, "the configuration for the node is empty")
		return
	}
	nodeConfig := base.ConfigModel{}
	err = json.Unmarshal(configRes, &nodeConfig)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	// 传入的ip地址必须与指定节点配置文件里的ip地址一致
	if command.Addr != nodeConfig.Node.Addr {
		result(w, false, nil, "the ip address configuration of the node does not match")
		return
	}
	// 传入的http端口号必须与指定节点配置文件里的http端口号一致
	if command.HttpPort != nodeConfig.Node.HttpPort {
		result(w, false, nil, "the http port configuration of the node does not match")
		return
	}
	// 传入的tcp端口号必须与指定节点配置文件里的tcp端口号一致
	if command.TcpPort != nodeConfig.Node.TcpPort {
		result(w, false, nil, "the tcp port configuration of the node does not match")
		return
	}

	err = service.AddNode(command.Addr, command.TcpPort, command.HttpPort)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	result(w, true, endpoint, "")
}

func removeNode(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	address := p.ByName("endpoint")
	err := service.RemoveNode(address)
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

func listNode(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	ns := service.ListNode()
	result(w, true, ns, "")
}

func listNamespace(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	ns := service.ListNamespace()
	result(w, true, ns, "")
}

func createNamespace(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	err := forwardToLeader(w, r)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	namespace := p.ByName("namespace")
	err = service.CreateNamespace(namespace)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	result(w, true, nil, "")
}

func deleteNamespace(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	err := forwardToLeader(w, r)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	namespace := p.ByName("namespace")
	err = service.DeleteNamespace(namespace)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	result(w, true, nil, "")
}

func putKV(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	err := forwardToLeader(w, r)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
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
	err := forwardToLeader(w, r)
	if err != nil {
		result(w, false, nil, err.Error())
		return
	}
	namespace := p.ByName("namespace")
	key := p.ByName("key")
	err = service.DeleteKV(namespace, key)
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

func forwardToLeader(w http.ResponseWriter, r *http.Request) error {
	// 如果该节点是leader，则无需转发请求
	if service.GetLeader() == fmt.Sprintf("%s:%v", base.Config().Node.Addr, base.Config().Node.HttpPort) {
		return nil
	} else {
		return base.HttpForward(w, r, fmt.Sprintf("http://%s%s", service.GetLeader(), r.URL.RequestURI()))
	}
}

package http_v2

import (
	"encoding/json"
	"fmt"
	"fraisedb/base"
	"fraisedb/service"
	"github.com/julienschmidt/httprouter"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"net/http"
	"strconv"
	"time"
)

func getHealth(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	_, err := w.Write([]byte("1"))
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
	}
}

func getConfig(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	marshal, _ := json.Marshal(base.Config())
	_, err := w.Write(marshal)
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
	}
}

func addNode(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	command, err := readNodeCommand(r)
	if err != nil {
		result(w, base.InterfaceLayerErrorCode, nil, err)
		return
	}

	endpoint := fmt.Sprintf("%s:%v", command.Addr, command.HttpPort)

	// 节点健康检查
	healthRes, err := base.HttpGet(fmt.Sprintf("http://%s/v2/health", endpoint))
	if err != nil {
		result(w, base.ClusterInvocationErrorCode, nil, err)
		return
	}
	if healthRes == nil || string(healthRes) != "1" {
		result(w, base.ClusterInvocationErrorCode, nil, errors.New("the node is unhealthy"))
		return
	}

	// 节点配置检查
	configRes, err := base.HttpGet(fmt.Sprintf("http://%s/v2/config", endpoint))
	if err != nil {
		result(w, base.ClusterInvocationErrorCode, nil, err)
		return
	}
	if configRes == nil || len(configRes) == 0 {
		result(w, base.ClusterInvocationErrorCode, nil, errors.New("the configuration for the node is empty"))
		return
	}
	nodeConfig := base.ConfigModel{}
	err = json.Unmarshal(configRes, &nodeConfig)
	if err != nil {
		result(w, base.ClusterInvocationErrorCode, nil, err)
		return
	}
	// 传入的ip地址必须与指定节点配置文件里的ip地址一致
	if command.Addr != nodeConfig.Node.Addr {
		result(w, base.ClusterInvocationErrorCode, nil, errors.New("the ip address configuration of the node does not match"))
		return
	}
	// 传入的http端口号必须与指定节点配置文件里的http端口号一致
	if command.HttpPort != nodeConfig.Node.HttpPort {
		result(w, base.ClusterInvocationErrorCode, nil, errors.New("the http port configuration of the node does not match"))
		return
	}
	// 传入的tcp端口号必须与指定节点配置文件里的tcp端口号一致
	if command.TcpPort != nodeConfig.Node.TcpPort {
		result(w, base.ClusterInvocationErrorCode, nil, errors.New("the tcp port configuration of the node does not match"))
		return
	}

	err = service.AddNode(command.Addr, command.TcpPort, command.HttpPort)
	if err != nil {
		result(w, base.ServiceLayerErrorCode, nil, err)
		return
	}
	result(w, base.SuccessCode, endpoint, nil)
}

func removeNode(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	address := p.ByName("endpoint")
	err := service.RemoveNode(address)
	if err != nil {
		result(w, base.ServiceLayerErrorCode, nil, err)
		return
	}
	result(w, base.SuccessCode, nil, nil)
}

func getLeader(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	leader := service.GetLeader()
	if len(leader) == 0 {
		result(w, base.ServiceLayerErrorCode, nil, errors.New("missing leader"))
		return
	}
	result(w, base.SuccessCode, leader, nil)
}

func listNode(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	ns := service.ListNode()
	result(w, base.SuccessCode, ns, nil)
}

func listNamespace(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	ns := service.ListNamespace()
	result(w, base.SuccessCode, ns, nil)
}

func createNamespace(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	err := forwardToLeader(w, r)
	if err != nil {
		result(w, base.ClusterInvocationErrorCode, nil, err)
		return
	}
	namespace := p.ByName("namespace")
	err = service.CreateNamespace(namespace)
	if err != nil {
		result(w, base.ServiceLayerErrorCode, nil, err)
		return
	}
	result(w, base.SuccessCode, nil, nil)
}

func deleteNamespace(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	err := forwardToLeader(w, r)
	if err != nil {
		result(w, base.ClusterInvocationErrorCode, nil, err)
		return
	}
	namespace := p.ByName("namespace")
	err = service.DeleteNamespace(namespace)
	if err != nil {
		result(w, base.ServiceLayerErrorCode, nil, err)
		return
	}
	result(w, base.SuccessCode, nil, nil)
}

func putKV(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	err := forwardToLeader(w, r)
	if err != nil {
		result(w, base.ClusterInvocationErrorCode, nil, err)
		return
	}
	namespace := p.ByName("namespace")
	key := p.ByName("key")
	command, err := readKVCommand(r)
	if err != nil {
		result(w, base.InterfaceLayerErrorCode, nil, err)
		return
	}
	var ddl int64 = 0
	if command.TTL > 0 {
		ddl = time.Now().Unix() + command.TTL
	}
	err = service.PutKV(namespace, key, command.Value, ddl)
	if err != nil {
		result(w, base.ServiceLayerErrorCode, nil, err)
		return
	}
	result(w, base.SuccessCode, ddl, nil)
}

func getKV(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	namespace := p.ByName("namespace")
	key := p.ByName("key")
	value, err := service.GetKV(namespace, key)
	if err != nil {
		result(w, base.ServiceLayerErrorCode, nil, err)
		return
	}
	result(w, base.SuccessCode, value, nil)
}

func deleteKV(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	err := forwardToLeader(w, r)
	if err != nil {
		result(w, base.ClusterInvocationErrorCode, nil, err)
		return
	}
	namespace := p.ByName("namespace")
	key := p.ByName("key")
	err = service.DeleteKV(namespace, key)
	if err != nil {
		result(w, base.ServiceLayerErrorCode, nil, err)
		return
	}
	result(w, base.SuccessCode, nil, nil)
}

func listKV(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	namespace := p.ByName("namespace")
	keyPrefix := p.ByName("keyPrefix")
	offsetStr := r.URL.Query().Get("offset")
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		result(w, base.InterfaceLayerErrorCode, nil, err)
		return
	}
	countStr := r.URL.Query().Get("count")
	count, err := strconv.ParseInt(countStr, 10, 64)
	if err != nil {
		result(w, base.InterfaceLayerErrorCode, nil, err)
		return
	}
	kvs, err := service.ListKV(namespace, keyPrefix, offset, count)
	if err != nil {
		result(w, base.ServiceLayerErrorCode, nil, err)
		return
	}
	result(w, base.SuccessCode, kvs, nil)
}

func forwardToLeader(w http.ResponseWriter, r *http.Request) error {
	// 如果该节点是leader，则无需转发请求
	if service.GetLeader() == fmt.Sprintf("%s:%v", base.Config().Node.Addr, base.Config().Node.HttpPort) {
		return nil
	} else {
		return base.HttpForward(w, r, fmt.Sprintf("http://%s%s", service.GetLeader(), r.URL.RequestURI()))
	}
}

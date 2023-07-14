package service

import (
	"fmt"
	"fraisedb/base"
	"fraisedb/cluster"
	"fraisedb/store"
	"github.com/syndtr/goleveldb/leveldb/errors"
)

// StartNode 启动节点
func StartNode() {
	base.Channel = make(chan []byte, 1000)
	err := base.CreatePath(base.Config().Store.Data)
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
		panic(err)
	}
	kvPath := fmt.Sprintf("%s/kv", base.Config().Store.Data)
	err = base.CreatePath(kvPath)
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
		panic(err)
	}
	base.NodeDB, err = store.NewDB(kvPath)
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
		panic(err)
	}
	base.Node, err = cluster.StartNode(base.Config().Node.First,
		fmt.Sprintf("%s:%v", base.Config().Node.Addr, base.Config().Node.TcpPort),
		fmt.Sprintf("%s/log", base.Config().Store.Data),
		fmt.Sprintf("%s/stable", base.Config().Store.Data),
		fmt.Sprintf("%s/snapshot", base.Config().Store.Data))
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
		panic(err)
	}
}

// AddNode 在领导者节点上添加新的节点
func AddNode(addr string, port int) error {
	if len(addr) == 0 {
		return errors.New("len(addr) == 0")
	}
	if port <= 0 {
		return errors.New("port <= 0")
	}
	return cluster.AddNode(base.Node, fmt.Sprintf("%s:%v", addr, port))
}

func GetLeader() string {
	return cluster.GetLeader(base.Node)
}

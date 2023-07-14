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
	err := base.CreatePath(base.Config().Store.Data)
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
		panic(err)
	}
	base.Channel = make(chan []byte, 1000)
	base.NodeDB, err = store.NewDB(fmt.Sprintf("%s/kv", base.Config().Store.Data))
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

func RemoveNode(nodeAddr string) error {
	return cluster.RemoveNode(base.Node, nodeAddr)
}

func GetLeader() string {
	return cluster.GetLeader(base.Node)
}

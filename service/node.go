package service

import (
	"fmt"
	"fraisedb/base"
	"fraisedb/cluster"
	"github.com/syndtr/goleveldb/leveldb/errors"
)

// StartNode 启动节点
func StartNode() {
	err := error(nil)
	base.Channel = make(chan []byte, 1000)
	base.Node, base.NodeDB, err = cluster.StartNode(base.Config().Node.First,
		fmt.Sprintf("%s:%v", base.Config().Node.Addr, base.Config().Node.TcpPort),
		base.Config().Node.LogStore,
		base.Config().Node.StableStore,
		base.Config().Node.SnapshotStore,
		base.Config().Node.KVStore)
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

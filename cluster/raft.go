package cluster

import (
	"fraisedb/base"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftBoltDB "github.com/hashicorp/raft-boltdb"
	"gopkg.in/yaml.v3"
	"net"
	"os"
	"time"
)

func StartNode(first bool, localAddr string, logStorePath string, stableStorePath string, snapshotStorePath string) (*raft.Raft, error) {

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(localAddr)
	raftConfig.Logger = hclog.Default()

	logStore, err := raftBoltDB.NewBoltStore(logStorePath)
	if err != nil {
		return nil, err
	}

	stableStore, err := raftBoltDB.NewBoltStore(stableStorePath)
	if err != nil {
		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(snapshotStorePath, 1, os.Stderr)
	if err != nil {
		return nil, err
	}

	localAddress, err := net.ResolveTCPAddr("tcp", localAddr)
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(localAddr, localAddress, 3, base.ConnectTimeout*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	fsm, err := newFsm(localAddr)
	if err != nil {
		return nil, err
	}

	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	if first {
		r.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		})
	}
	return r, nil
}

func AddNode(leader *raft.Raft, nodeAddr string) error {
	f := leader.AddVoter(raft.ServerID(nodeAddr), raft.ServerAddress(nodeAddr), 0, base.ConnectTimeout*time.Second)
	return f.Error()
}

func RemoveNode(leader *raft.Raft, nodeAddr string) error {
	f := leader.RemoveServer(raft.ServerID(nodeAddr), 0, base.ConnectTimeout*time.Second)
	return f.Error()
}

func GetLeader(node *raft.Raft) string {
	addr, _ := node.LeaderWithID()
	return string(addr)
}

type ApplyLogModel struct {
	Method int    `yaml:"m" json:"method"`
	Key    string `yaml:"k" json:"key"`
	Value  string `yaml:"v" json:"value"`
	DDL    int64  `yaml:"d" json:"ddl"`
}

func ApplyLog(node *raft.Raft, method int, key string, value string, ddl int64) error {
	log := ApplyLogModel{
		Method: method,
		Key:    key,
		Value:  value,
		DDL:    ddl,
	}
	marshal, err := yaml.Marshal(log)
	if err != nil {
		return err
	}
	node.ApplyLog(raft.Log{
		Data: marshal,
	}, base.ConnectTimeout*time.Second)
	return nil
}

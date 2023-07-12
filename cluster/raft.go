package cluster

import (
	"FraiseDB/base"
	"FraiseDB/store"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftBoltDB "github.com/hashicorp/raft-boltdb"
	"gopkg.in/yaml.v3"
	"net"
	"os"
)

func StartNode(first bool, localAddr string, logStorePath string, stableStorePath string, snapshotStorePath string, kvStorePath string) (*raft.Raft, store.DB, error) {

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(localAddr)
	raftConfig.Logger = hclog.Default()

	logStore, err := raftBoltDB.NewBoltStore(logStorePath)
	if err != nil {
		return nil, nil, err
	}

	stableStore, err := raftBoltDB.NewBoltStore(stableStorePath)
	if err != nil {
		return nil, nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(snapshotStorePath, 1, os.Stderr)
	if err != nil {
		return nil, nil, err
	}

	localAddress, err := net.ResolveTCPAddr("tcp", localAddr)
	if err != nil {
		return nil, nil, err
	}

	transport, err := raft.NewTCPTransport(localAddr, localAddress, 3, base.ConnectTimeout, os.Stderr)
	if err != nil {
		return nil, nil, err
	}

	fsm, db, err := newFsm(localAddr, kvStorePath)
	if err != nil {
		return nil, nil, err
	}

	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, nil, err
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
	return r, db, nil
}

func AddNode(leader *raft.Raft, newNodeAddr string) error {
	f := leader.AddVoter(raft.ServerID(newNodeAddr), raft.ServerAddress(newNodeAddr), 0, base.ConnectTimeout)
	return f.Error()
}

type ApplyLogModel struct {
	Method int    `yaml:"m"`
	Key    string `yaml:"k"`
	Value  string `yaml:"v"`
	DDL    int64  `yaml:"d"`
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
	}, base.ConnectTimeout)
	return nil
}

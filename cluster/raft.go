package cluster

import (
	"fmt"
	"fraisedb/base"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftBoltDB "github.com/hashicorp/raft-boltdb"
	"gopkg.in/yaml.v3"
	"net"
	"os"
	"sync"
	"time"
)

func StartNode(first bool, tcpHost string, httpHost string, logStorePath string, stableStorePath string, snapshotStorePath string) (*raft.Raft, error) {

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(httpHost)
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

	localAddress, err := net.ResolveTCPAddr("tcp", tcpHost)
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(tcpHost, localAddress, 3, base.ConnectTimeout3*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	fsm, err := newFsm()
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

func AddNode(leader *raft.Raft, tcpHost string, httpHost string) error {
	f := leader.AddVoter(raft.ServerID(httpHost), raft.ServerAddress(tcpHost), 0, base.ConnectTimeout3*time.Second)
	return f.Error()
}

func RemoveNode(leader *raft.Raft, httpHost string) error {
	f := leader.RemoveServer(raft.ServerID(httpHost), 0, base.ConnectTimeout3*time.Second)
	return f.Error()
}

func GetLeader(node *raft.Raft) string {
	_, id := node.LeaderWithID()
	return string(id)
}

type NodeInfoModel struct {
	Endpoint string `json:"endpoint"`
	Health   bool   `json:"health"`
	Leader   bool   `json:"leader"`
}

func ListNode(node *raft.Raft) []NodeInfoModel {
	if len(node.GetConfiguration().Configuration().Servers) == 0 {
		return nil
	}
	leaderID := GetLeader(node)
	var ns []NodeInfoModel
	var wg sync.WaitGroup
	wg.Add(len(node.GetConfiguration().Configuration().Servers))
	for _, v := range node.GetConfiguration().Configuration().Servers {
		go func(v raft.Server) {
			endpoint := string(v.ID)
			health := false
			leader := false
			res, err := base.HttpGet(fmt.Sprintf("http://%s/v2/health", endpoint))
			if err == nil && res != nil && string(res) == "1" {
				health = true
			}
			if endpoint == leaderID {
				leader = true
			}
			ns = append(ns, NodeInfoModel{
				Endpoint: endpoint,
				Health:   health,
				Leader:   leader,
			})
			wg.Done()
		}(v)
	}
	wg.Wait()
	return ns
}

type ApplyLogModel struct {
	// 0-删除key、1-新建key、10-删除namespace、11-新建namespace
	Method    int    `yaml:"m" json:"method"`
	Namespace string `yaml:"n" json:"namespace"`
	Key       string `yaml:"k" json:"key"`
	Value     string `yaml:"v" json:"value"`
	DDL       int64  `yaml:"d" json:"ddl"`
}

func ApplyLog(node *raft.Raft, namespace string, method int, key string, value string, ddl int64) error {
	log := ApplyLogModel{
		Method:    method,
		Namespace: namespace,
		Key:       key,
		Value:     value,
		DDL:       ddl,
	}
	marshal, err := yaml.Marshal(log)
	if err != nil {
		return err
	}
	node.ApplyLog(raft.Log{
		Data: marshal,
	}, base.ConnectTimeout30*time.Second)
	return nil
}

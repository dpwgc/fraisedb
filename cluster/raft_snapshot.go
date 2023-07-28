package cluster

import (
	"fraisedb/base"
	"github.com/hashicorp/raft"
	"gopkg.in/yaml.v3"
)

type Snapshot struct {
}

func newSnapshot() raft.FSMSnapshot {
	return &Snapshot{}
}

type KVSnapshotModel struct {
	Namespace string `yaml:"n"`
	Key       string `yaml:"k"`
	Value     string `yaml:"v"`
	DDL       int64  `yaml:"d"`
}

// Persist saves the FSM snapshot out to the given sink.
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	err := error(nil)
	defer func() {
		if err != nil {
			base.LogHandler.Println(base.LogErrorTag, err)
			err = sink.Cancel()
			if err != nil {
				base.LogHandler.Println(base.LogErrorTag, err)
			}
		}
	}()
	var kvSnaps []KVSnapshotModel
	ns := base.NodeDB.ListNamespace()
	for _, n := range ns {
		kvs, err := base.NodeDB.ListKV(n, "", 0, 0)
		if err != nil {
			return err
		}
		for _, kv := range kvs {
			kvSnap := KVSnapshotModel{
				Namespace: n,
				Key:       kv.Key,
				Value:     kv.Value,
				DDL:       kv.DDL,
			}
			kvSnaps = append(kvSnaps, kvSnap)
		}
	}
	marshal, err := yaml.Marshal(kvSnaps)
	if err != nil {
		return err
	}
	if _, err = sink.Write(marshal); err != nil {
		return err
	}
	if err = sink.Close(); err != nil {
		return err
	}
	return nil
}
func (s *Snapshot) Release() {}

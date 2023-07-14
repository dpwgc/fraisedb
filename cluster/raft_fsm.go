package cluster

import (
	"fraisedb/base"
	"github.com/hashicorp/raft"
	"gopkg.in/yaml.v3"
	"io"
	"sync"
	"time"
)

var mutex sync.Mutex

type StorageFSM struct {
	Id string
	l  *sync.Mutex
}

func newFsm(id string) (raft.FSM, error) {
	return &StorageFSM{
		id,
		&mutex,
	}, nil
}

func (c *StorageFSM) Apply(log *raft.Log) interface{} {
	al := ApplyLogModel{}
	err := yaml.Unmarshal(log.Data, &al)
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
		return nil
	}
	if al.DDL > 0 && time.Now().Unix() > al.DDL {
		return nil
	}
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
		return nil
	}
	if al.Method == 1 {
		err = base.NodeDB.PutKV(al.Namespace, al.Key, al.Value, al.DDL)
	} else {
		err = base.NodeDB.DeleteKV(al.Namespace, al.Key)
	}
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
	}
	if time.Now().Unix()-base.ConnectTimeout <= log.AppendedAt.Unix() {
		base.Channel <- log.Data
	}
	return nil
}

func (c *StorageFSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (c *StorageFSM) Restore(rc io.ReadCloser) error {
	return nil
}

package cluster

import (
	"fraisedb/base"
	"fraisedb/store"
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

func newFsm(id string, path string) (raft.FSM, store.DB, error) {
	db, err := store.NewDB(path)
	if err != nil {
		return nil, nil, err
	}
	return &StorageFSM{
		id,
		&mutex,
	}, db, nil
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
	if al.Method == 1 {
		err = base.NodeDB.Put(al.Key, al.Value, al.DDL)
	} else {
		err = base.NodeDB.Delete(al.Key)
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

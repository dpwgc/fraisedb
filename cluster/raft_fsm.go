package cluster

import (
	"fraisedb/base"
	"github.com/hashicorp/raft"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"gopkg.in/yaml.v3"
	"io"
	"sync"
	"time"
)

var mutex sync.Mutex

type StorageFSM struct {
}

func newFsm() (raft.FSM, error) {
	return &StorageFSM{}, nil
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
	// 0-删除key、1-新建key、10-删除namespace、11-新建namespace
	switch al.Method {
	case 0:
		err = base.NodeDB.DeleteKV(al.Namespace, al.Key)
		break
	case 1:
		err = base.NodeDB.PutKV(al.Namespace, al.Key, al.SaveType, al.Value, al.Incr, al.DDL)
		break
	case 10:
		err = base.NodeDB.DeleteNamespace(al.Namespace)
		break
	case 11:
		err = base.NodeDB.CreateNamespace(al.Namespace)
		break
	default:
		err = errors.New("apply log method error")
	}
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
	}
	if time.Now().Unix()-base.ConnectTimeout30 <= log.AppendedAt.Unix() && al.Method < 2 {
		base.Channel <- log.Data
	}
	return nil
}

func (c *StorageFSM) Snapshot() (raft.FSMSnapshot, error) {
	return newSnapshot(), nil
}

func (c *StorageFSM) Restore(rc io.ReadCloser) error {
	var kvSnaps []KVSnapshotModel
	if err := yaml.NewDecoder(rc).Decode(&kvSnaps); err != nil {
		return err
	}
	mutex.Lock()
	for _, kvSnap := range kvSnaps {
		err := base.NodeDB.CreateNamespace(kvSnap.Namespace)
		if err != nil {
			return err
		}
		err = base.NodeDB.PutKV(kvSnap.Namespace, kvSnap.Key, 0, kvSnap.Value, 0, kvSnap.DDL)
		if err != nil {
			return err
		}
	}
	defer mutex.Unlock()
	return nil
}

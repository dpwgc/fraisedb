package store

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/util"
	"gopkg.in/yaml.v3"
	"sync"
	"time"
)

type levelDB struct {
	DB
	path  string
	dbMap map[string]*leveldb.DB
}

func newLevelDB(path string) (DB, error) {
	s := &levelDB{
		path:  path,
		dbMap: make(map[string]*leveldb.DB, 100),
	}
	go backgroundCleanTask(s)
	return s, nil
}

func (s *levelDB) GetKV(namespace string, key string) (ValueModel, error) {
	vm := ValueModel{}
	err := autoCreateNamespace(s, namespace)
	if err != nil {
		return vm, errors.New("namespace create error")
	}
	value, err := s.dbMap[namespace].Get([]byte(key), nil)
	if err != nil {
		return vm, err
	}
	err = yaml.Unmarshal(value, &vm)
	if err != nil {
		backgroundDelKey(s.dbMap[namespace], key)
		return vm, err
	}
	if vm.DDL > 0 && time.Now().Unix() > vm.DDL {
		backgroundDelKey(s.dbMap[namespace], key)
		return vm, nil
	}
	return vm, nil
}

func (s *levelDB) PutKV(namespace string, key string, value string, ddl int64) error {
	err := autoCreateNamespace(s, namespace)
	if err != nil {
		return errors.New("namespace create error")
	}
	vm := ValueModel{
		Value: value,
		DDL:   ddl,
	}
	marshal, err := yaml.Marshal(vm)
	if err != nil {
		return err
	}
	return s.dbMap[namespace].Put([]byte(key), marshal, nil)
}

func (s *levelDB) DeleteKV(namespace string, key string) error {
	err := autoCreateNamespace(s, namespace)
	if err != nil {
		return errors.New("namespace create error")
	}
	return s.dbMap[namespace].Delete([]byte(key), nil)
}

func (s *levelDB) ListKV(namespace string, keyPrefix string, limit int64) (map[string]ValueModel, error) {
	err := autoCreateNamespace(s, namespace)
	if err != nil {
		return nil, errors.New("namespace create error")
	}
	var deleteKeys []string
	var i int64 = 0
	var mapInitLimit int64 = 100
	if limit > 0 {
		mapInitLimit = limit
	}
	var kvs = make(map[string]ValueModel, mapInitLimit)
	var bytesPrefix *util.Range = nil
	if len(keyPrefix) > 0 {
		bytesPrefix = util.BytesPrefix([]byte(keyPrefix))
	}
	iter := s.dbMap[namespace].NewIterator(bytesPrefix, nil)
	for iter.Next() {
		i = i + 1
		if i > limit && limit > 0 {
			break
		}
		vm := ValueModel{}
		key := string(iter.Key())
		err := yaml.Unmarshal(iter.Value(), &vm)
		if err != nil {
			deleteKeys = append(deleteKeys, key)
			continue
		}
		if vm.DDL > 0 && time.Now().Unix() > vm.DDL {
			deleteKeys = append(deleteKeys, key)
			continue
		}
		kvs[key] = vm
	}
	iter.Release()
	for _, k := range deleteKeys {
		backgroundDelKey(s.dbMap[namespace], k)
	}
	return kvs, nil
}

func backgroundDelKey(db *leveldb.DB, key string) {
	_ = db.Delete([]byte(key), nil)
}

func backgroundCleanTask(s *levelDB) {
	for {
		time.Sleep(1 * time.Minute)
		for _, db := range s.dbMap {
			backgroundClean(db)
		}
	}
}

func backgroundClean(db *leveldb.DB) {
	var deleteKeys []string
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		vm := ValueModel{}
		key := string(iter.Key())
		err := yaml.Unmarshal(iter.Value(), &vm)
		if err != nil {
			deleteKeys = append(deleteKeys, key)
			continue
		}
		if vm.DDL > 0 && time.Now().Unix() > vm.DDL {
			deleteKeys = append(deleteKeys, key)
			continue
		}
	}
	iter.Release()
	for _, k := range deleteKeys {
		backgroundDelKey(db, k)
	}
}

var namespaceLock sync.Mutex

func autoCreateNamespace(s *levelDB, namespace string) error {
	if s.dbMap[namespace] == nil {
		db, err := leveldb.OpenFile(fmt.Sprintf("%s/%s", s.path, namespace), nil)
		if err != nil {
			return err
		}
		if s.dbMap[namespace] == nil {
			namespaceLock.Lock()
			s.dbMap[namespace] = db
			namespaceLock.Unlock()
		}
	}
	return nil
}

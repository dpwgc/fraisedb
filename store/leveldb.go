package store

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"gopkg.in/yaml.v3"
	"time"
)

type levelDB struct {
	DB
	db *leveldb.DB
}

func newLevelDB(path string) (DB, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	s := &levelDB{
		db: db,
	}
	go backgroundCleanTask(s)
	return s, nil
}

func (s *levelDB) Get(key string) (ValueModel, error) {
	vm := ValueModel{}
	value, err := s.db.Get([]byte(key), nil)
	if err != nil {
		return vm, err
	}
	err = yaml.Unmarshal(value, &vm)
	if err != nil {
		backgroundDelKey(s, key)
		return vm, err
	}
	if vm.DDL > 0 && time.Now().Unix() > vm.DDL {
		backgroundDelKey(s, key)
		return vm, nil
	}
	return vm, nil
}

func (s *levelDB) Put(key string, value string, ddl int64) error {
	vm := ValueModel{
		Value: value,
		DDL:   ddl,
	}
	marshal, err := yaml.Marshal(vm)
	if err != nil {
		return err
	}
	return s.db.Put([]byte(key), marshal, nil)
}

func (s *levelDB) Delete(key string) error {
	return s.db.Delete([]byte(key), nil)
}

func (s *levelDB) List(keyPrefix string, limit int64) (map[string]ValueModel, error) {
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
	iter := s.db.NewIterator(bytesPrefix, nil)
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
		backgroundDelKey(s, k)
	}
	return kvs, nil
}

func backgroundDelKey(s *levelDB, key string) {
	_ = s.db.Delete([]byte(key), nil)
}

func backgroundCleanTask(s *levelDB) {
	for {
		time.Sleep(1 * time.Minute)
		backgroundClean(s)
	}
}

func backgroundClean(s *levelDB) {
	var deleteKeys []string
	iter := s.db.NewIterator(nil, nil)
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
		backgroundDelKey(s, k)
	}
}

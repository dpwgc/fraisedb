package store

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/util"
	"gopkg.in/yaml.v3"
	"os"
	"sync"
	"time"
)

type levelDB struct {
	DB
	path  string
	dbMap map[string]*leveldb.DB
}

var namespaceLock sync.Mutex

func newLevelDB(path string) (DB, error) {
	s := &levelDB{
		path:  path,
		dbMap: make(map[string]*leveldb.DB, 100),
	}
	go backgroundCleanTask(s)
	return s, nil
}

func (s *levelDB) ListNamespace() []string {
	var ns []string
	for n, _ := range s.dbMap {
		ns = append(ns, n)
	}
	return ns
}

func (s *levelDB) NamespaceNotExist(namespace string) bool {
	return s.dbMap[namespace] == nil
}

func (s *levelDB) CreateNamespace(namespace string) error {
	if s.dbMap[namespace] == nil {
		namespaceLock.Lock()
		db, err := leveldb.OpenFile(fmt.Sprintf("%s/%s", s.path, namespace), nil)
		if err != nil {
			return err
		}
		s.dbMap[namespace] = db
		namespaceLock.Unlock()
	}
	return nil
}

func (s *levelDB) DeleteNamespace(namespace string) error {
	if s.dbMap[namespace] != nil {
		namespaceLock.Lock()
		err := s.dbMap[namespace].Close()
		if err != nil {
			return err
		}
		err = os.RemoveAll(fmt.Sprintf("%s/%s", s.path, namespace))
		if err != nil {
			return err
		}
		delete(s.dbMap, namespace)
		namespaceLock.Unlock()
	}
	return nil
}

func (s *levelDB) GetKV(namespace string, key string) (KvDTO, error) {
	vm := ValueModel{}
	value, err := s.dbMap[namespace].Get([]byte(key), nil)
	if err != nil {
		return KvDTO{}, err
	}
	err = yaml.Unmarshal(value, &vm)
	if err != nil {
		backgroundDelKey(s.dbMap[namespace], key)
		return KvDTO{}, err
	}
	if vm.DDL > 0 && time.Now().Unix() > vm.DDL {
		backgroundDelKey(s.dbMap[namespace], key)
		return KvDTO{}, nil
	}
	return KvDTO{
		Key:   key,
		Value: vm.Value,
		DDL:   vm.DDL,
	}, nil
}

func (s *levelDB) PutKV(namespace string, key string, value string, ddl int64) error {
	if s.dbMap[namespace] == nil {
		return errors.New("namespace not exist")
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
	if s.dbMap[namespace] == nil {
		return errors.New("namespace not exist")
	}
	return s.dbMap[namespace].Delete([]byte(key), nil)
}

func (s *levelDB) ListKV(namespace string, keyPrefix string, offset int64, count int64) ([]KvDTO, error) {
	var o int64 = 0
	var c int64 = 0
	var kvs []KvDTO
	var bytesPrefix *util.Range = nil
	if len(keyPrefix) > 0 {
		bytesPrefix = util.BytesPrefix([]byte(keyPrefix))
	}
	iter := s.dbMap[namespace].NewIterator(bytesPrefix, nil)
	for iter.Next() {
		vm := ValueModel{}
		key := string(iter.Key())
		err := yaml.Unmarshal(iter.Value(), &vm)
		if err != nil {
			backgroundDelKey(s.dbMap[namespace], key)
			continue
		}
		if vm.DDL > 0 && time.Now().Unix() > vm.DDL {
			backgroundDelKey(s.dbMap[namespace], key)
			continue
		}
		// 到指定游标后再取值
		if o < offset {
			o = o + 1
			continue
		}
		// 取值区间长度限制
		c = c + 1
		if c > count && count > 0 {
			break
		}
		kvs = append(kvs, KvDTO{
			Key:   key,
			Value: vm.Value,
			DDL:   vm.DDL,
		})
	}
	iter.Release()
	return kvs, nil
}

func backgroundDelKey(db *leveldb.DB, key string) {
	if db != nil {
		_ = db.Delete([]byte(key), nil)
	}
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
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		vm := ValueModel{}
		key := string(iter.Key())
		err := yaml.Unmarshal(iter.Value(), &vm)
		if err != nil {
			backgroundDelKey(db, key)
			continue
		}
		if vm.DDL > 0 && time.Now().Unix() > vm.DDL {
			backgroundDelKey(db, key)
			continue
		}
	}
	iter.Release()
}

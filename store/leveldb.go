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

type valueModel struct {
	Content string `yaml:"c"`
	DDL     int64  `yaml:"d"`
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

func (s *levelDB) Get(key string) (string, error) {
	value, err := s.db.Get([]byte(key), nil)
	if err != nil {
		return "", err
	}
	vo := &valueModel{}
	err = yaml.Unmarshal(value, &vo)
	if err != nil {
		backgroundDelKey(s, key)
		return "", err
	}
	if vo.DDL > 0 && time.Now().Unix() > vo.DDL {
		backgroundDelKey(s, key)
		return "", nil
	}
	return vo.Content, nil
}

func (s *levelDB) Put(key string, value string, ddl int64) error {
	vo := valueModel{
		Content: value,
		DDL:     ddl,
	}
	marshal, err := yaml.Marshal(vo)
	if err != nil {
		return err
	}
	return s.db.Put([]byte(key), marshal, nil)
}

func (s *levelDB) Delete(key string) error {
	return s.db.Delete([]byte(key), nil)
}

func (s *levelDB) List(keyPrefix string, limit int64) (map[string]string, error) {
	var deleteKeys []string
	var i int64 = 0
	var kvs = make(map[string]string, limit)
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
		vo := &valueModel{}
		key := string(iter.Key())
		err := yaml.Unmarshal(iter.Value(), &vo)
		if err != nil {
			deleteKeys = append(deleteKeys, key)
			continue
		}
		if vo.DDL > 0 && time.Now().Unix() > vo.DDL {
			deleteKeys = append(deleteKeys, key)
			continue
		}
		kvs[key] = vo.Content
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
		vo := &valueModel{}
		key := string(iter.Key())
		err := yaml.Unmarshal(iter.Value(), &vo)
		if err != nil {
			deleteKeys = append(deleteKeys, key)
			continue
		}
		if vo.DDL > 0 && time.Now().Unix() > vo.DDL {
			deleteKeys = append(deleteKeys, key)
			continue
		}
	}
	iter.Release()
	for _, k := range deleteKeys {
		backgroundDelKey(s, k)
	}
}

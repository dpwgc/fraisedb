package service

import (
	"fraisedb/base"
	"fraisedb/cluster"
	"github.com/syndtr/goleveldb/leveldb/errors"
)

func GetKV(key string) (map[string]interface{}, error) {
	if len(key) == 0 {
		return nil, errors.New("len(key) == 0")
	}
	return base.NodeDB.Get(key)
}

func PutKV(key string, value string, ddl int64) error {
	if len(key) == 0 {
		return errors.New("len(key) == 0")
	}
	if len(value) == 0 {
		return errors.New("len(value) == 0")
	}
	return cluster.ApplyLog(base.Node, 1, key, value, ddl)
}

func DeleteKV(key string) error {
	if len(key) == 0 {
		return errors.New("len(key) == 0")
	}
	return cluster.ApplyLog(base.Node, -1, key, "", 0)
}

func ListKV(keyPrefix string, limit int64) (map[string]map[string]interface{}, error) {
	return base.NodeDB.List(keyPrefix, limit)
}

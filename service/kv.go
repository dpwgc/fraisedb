package service

import (
	"fraisedb/base"
	"fraisedb/cluster"
	"fraisedb/store"
	"github.com/syndtr/goleveldb/leveldb/errors"
)

func GetKV(namespace string, key string) (store.ValueModel, error) {
	if len(namespace) == 0 {
		return store.ValueModel{}, errors.New("len(namespace) == 0")
	}
	if len(key) == 0 {
		return store.ValueModel{}, errors.New("len(key) == 0")
	}
	return base.NodeDB.GetKV(namespace, key)
}

func PutKV(namespace string, key string, value string, ddl int64) error {
	if len(namespace) == 0 {
		return errors.New("len(namespace) == 0")
	}
	if len(key) == 0 {
		return errors.New("len(key) == 0")
	}
	if len(value) == 0 {
		return errors.New("len(value) == 0")
	}
	return cluster.ApplyLog(base.Node, namespace, 1, key, value, ddl)
}

func DeleteKV(namespace string, key string) error {
	if len(namespace) == 0 {
		return errors.New("len(namespace) == 0")
	}
	if len(key) == 0 {
		return errors.New("len(key) == 0")
	}
	return cluster.ApplyLog(base.Node, namespace, 0, key, "", 0)
}

func ListKV(namespace string, keyPrefix string, limit int64) (map[string]store.ValueModel, error) {
	if len(namespace) == 0 {
		return nil, errors.New("len(namespace) == 0")
	}
	return base.NodeDB.ListKV(namespace, keyPrefix, limit)
}

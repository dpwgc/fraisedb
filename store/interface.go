package store

func NewDB(path string) (DB, error) {
	return newLevelDB(path)
}

type DB interface {
	Get(key string) (string, error)
	Put(key string, value string, ddl int64) error
	Delete(key string) error
	List(keyPrefix string, limit int64) (map[string]string, error)
}

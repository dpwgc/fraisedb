package store

func NewDB(path string) (DB, error) {
	return newLevelDB(path)
}

type ValueModel struct {
	Value string `yaml:"c" json:"value"`
	DDL   int64  `yaml:"d" json:"ddl"`
}

type DB interface {
	Get(key string) (ValueModel, error)
	Put(key string, value string, ddl int64) error
	Delete(key string) error
	List(keyPrefix string, limit int64) (map[string]ValueModel, error)
}

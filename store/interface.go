package store

func NewDB(path string) (DB, error) {
	return newLevelDB(path)
}

type ValueModel struct {
	Value string `yaml:"c" json:"value"`
	DDL   int64  `yaml:"d" json:"ddl"`
}

type KvDTO struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	DDL   int64  `json:"ddl"`
}

type DB interface {
	ListNamespace() []string
	NamespaceNotExist(namespace string) bool
	CreateNamespace(namespace string) error
	DeleteNamespace(namespace string) error
	GetKV(namespace string, key string) (KvDTO, error)
	PutKV(namespace string, key string, saveType int, value string, incr int64, ddl int64) error
	DeleteKV(namespace string, key string) error
	ListKV(namespace string, keyPrefix string, offset int64, count int64) ([]KvDTO, error)
}

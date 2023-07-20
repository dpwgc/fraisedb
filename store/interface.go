package store

func NewDB(path string) (DB, error) {
	return newLevelDB(path)
}

type ValueModel struct {
	Value string `yaml:"c" json:"value"`
	DDL   int64  `yaml:"d" json:"ddl"`
}

type DB interface {
	ListNamespace() []string
	NamespaceNotExist(namespace string) bool
	CreateNamespace(namespace string) error
	DeleteNamespace(namespace string) error
	GetKV(namespace string, key string) (ValueModel, error)
	PutKV(namespace string, key string, value string, ddl int64) error
	DeleteKV(namespace string, key string) error
	ListKV(namespace string, keyPrefix string, limit int64) (map[string]ValueModel, error)
}

package storage

type Storage interface {
	Save(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
}

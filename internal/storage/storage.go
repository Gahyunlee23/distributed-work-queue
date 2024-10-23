package storage

import (
	"context"
	"errors"
)

// ErrKeyNotFound when the key is not found
var ErrKeyNotFound = errors.New("key not found")

// Storage is a basic interface of data storage
type Storage interface {
	Set(ctx context.Context, key string, value []byte, ttl int) error

	Get(ctx context.Context, key string) ([]byte, error)

	Delete(ctx context.Context, key string) error

	Close() error
}

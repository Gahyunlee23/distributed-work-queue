package storage

import (
	"context"
	"sync"
	"time"
)

type MemoryStorage struct {
	mu       sync.RWMutex
	data     map[string][]byte
	expiries map[string]time.Time
}

type item struct {
	value    []byte
	expireAt time.Time
}

func NewMemoryStorage() *MemoryStorage {
	ms := &MemoryStorage{
		data:     make(map[string][]byte),
		expiries: make(map[string]time.Time),
	}

	// clean up the expired item with goroutine
	go ms.cleanupLoop()

	return ms
}

func (ms *MemoryStorage) Set(ctx context.Context, key string, value []byte, ttl int) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		ms.data[key] = value
		if ttl > 0 {
			ms.expiries[key] = time.Now().Add(time.Duration(ttl) * time.Second)
		}
		return nil
	}
}

func (ms *MemoryStorage) Get(ctx context.Context, key string) ([]byte, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		value, exists := ms.data[key]
		if !exists {
			return nil, ErrKeyNotFound
		}

		if expireAt, hasExpiry := ms.expiries[key]; hasExpiry && time.Now().After(expireAt) {
			delete(ms.data, key)
			delete(ms.expiries, key)
			return nil, ErrKeyNotFound
		}

		return value, nil
	}
}

func (ms *MemoryStorage) Delete(ctx context.Context, key string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		delete(ms.data, key)
		delete(ms.expiries, key)
		return nil
	}
}

func (ms *MemoryStorage) Close() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.data = nil
	ms.expiries = nil
	return nil
}

// cleanupLoop is a goroutine for deleting expired item
func (ms *MemoryStorage) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		ms.cleanup()
	}
}

func (ms *MemoryStorage) cleanup() {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	now := time.Now()
	for key, expireAt := range ms.expiries {
		if now.After(expireAt) {
			delete(ms.data, key)
			delete(ms.expiries, key)
		}
	}
}

package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Gahyunlee23/distributed-work-queue/internal/config"
	"github.com/go-redis/redis/v8"
)

type RedisStorage struct {
	client *redis.Client
}

func NewRedisStorage(cfg config.RedisConfig) (*RedisStorage, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Redis 연결 테스트
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return &RedisStorage{
		client: client,
	}, nil
}

func (rs *RedisStorage) Set(ctx context.Context, key string, value []byte, ttl int) error {
	if ttl > 0 {
		return rs.client.Set(ctx, key, value, time.Duration(ttl)*time.Second).Err()
	}
	return rs.client.Set(ctx, key, value, 0).Err()
}

func (rs *RedisStorage) Get(ctx context.Context, key string) ([]byte, error) {
	value, err := rs.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrKeyNotFound
	}
	return value, err
}

func (rs *RedisStorage) Delete(ctx context.Context, key string) error {
	return rs.client.Del(ctx, key).Err()
}

func (rs *RedisStorage) Close() error {
	return rs.client.Close()
}

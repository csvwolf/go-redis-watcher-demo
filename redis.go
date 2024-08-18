package main

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
)

type RedisClient struct {
	client *redis.Client
}

func NewClient(options *redis.Options) *RedisClient {
	client := redis.NewClient(options)
	return &RedisClient{
		client: client,
	}
}

func (r *RedisClient) GetClient() *redis.Client {
	return r.client
}

func (r *RedisClient) SetKey(ctx context.Context, key string, value interface{}, expire time.Duration) {
	r.client.Set(ctx, key, value, expire)
}

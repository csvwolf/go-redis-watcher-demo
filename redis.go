package main

import (
	"github.com/redis/go-redis/v9"
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

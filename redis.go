package main

import (
	"context"
	"github.com/redis/go-redis/v9"
	"sync"
	"time"
)

type RedisClient interface {
	Close() error
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd
	ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd
	PSubscribe(ctx context.Context, channels ...string) []*redis.PubSub
}

type RedisSingleClient struct {
	client *redis.Client
}

func NewSingleClient(r *redis.Client) RedisClient {
	return &RedisSingleClient{
		client: r,
	}
}

func (r *RedisSingleClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return r.client.Set(ctx, key, value, expiration)
}

func (r *RedisSingleClient) ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return r.client.ZRem(ctx, key, members...)
}

func (r *RedisSingleClient) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	return r.client.ZAdd(ctx, key, members...)
}

func (r *RedisSingleClient) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return r.client.ZRangeByScore(ctx, key, opt)
}

func (r *RedisSingleClient) Close() error {
	return r.client.Close()
}

func (r *RedisSingleClient) PSubscribe(ctx context.Context, channels ...string) []*redis.PubSub {
	return []*redis.PubSub{r.client.PSubscribe(ctx, channels...)}
}

type RedisClusterClient struct {
	client *redis.ClusterClient
}

func NewClusterClient(r *redis.ClusterClient) RedisClient {
	return &RedisClusterClient{
		client: r,
	}
}

func (r *RedisClusterClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return r.client.Set(ctx, key, value, expiration)
}

func (r *RedisClusterClient) ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return r.client.ZRem(ctx, key, members...)
}

func (r *RedisClusterClient) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	return r.client.ZAdd(ctx, key, members...)
}

func (r *RedisClusterClient) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return r.client.ZRangeByScore(ctx, key, opt)
}

func (r *RedisClusterClient) Close() error {
	return r.client.Close()
}

func (r *RedisClusterClient) PSubscribe(ctx context.Context, channels ...string) []*redis.PubSub {
	var (
		pubsubs []*redis.PubSub
		mut     sync.Mutex
	)
	r.client.ForEachShard(ctx, func(ctx context.Context, client *redis.Client) error {
		// Function 是并发的，需要加锁
		mut.Lock()
		pubsubs = append(pubsubs, client.PSubscribe(ctx, channels...))
		mut.Unlock()
		return nil
	})
	return pubsubs
}

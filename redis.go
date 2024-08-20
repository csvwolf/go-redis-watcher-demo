package main

import (
	"context"
	"github.com/redis/go-redis/v9"
	"strconv"
	"sync"
	"time"
)

type RedisClient interface {
	Close() error
	QueueKey() string
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	RemoveFromQueue(ctx context.Context, members ...interface{}) *redis.IntCmd
	GetKeysByTime(ctx context.Context, endTime time.Time) ([]string, error)
	PSubscribe(ctx context.Context, channels ...string) []*redis.PubSub
}

type RedisSingleClient struct {
	client   *redis.Client
	queueKey string
}

type RedisConfig struct {
	QueueKey string
}

func (config *RedisConfig) GetQueueKey() string {
	if config == nil || config.QueueKey == "" {
		return "job:delayed"
	}
	return config.QueueKey
}

func NewSingleClient(r *redis.Client, config *RedisConfig) RedisClient {
	return &RedisSingleClient{
		client:   r,
		queueKey: config.GetQueueKey(),
	}
}

func (r *RedisSingleClient) QueueKey() string {
	return r.queueKey
}

func (r *RedisSingleClient) Set(ctx context.Context, key string, value interface{}, expire time.Duration) error {
	var (
		err error
		now = time.Now().Add(expire)
	)

	if err = r.client.Set(ctx, key, value, expire).Err(); err != nil {
		return err
	}

	// 队列用于补偿
	if err = r.client.ZAdd(ctx, r.queueKey, redis.Z{Member: key, Score: float64(now.UnixMilli())}).Err(); err != nil {
		return err
	}
	return nil
}

func (r *RedisSingleClient) GetKeysByTime(ctx context.Context, endTime time.Time) ([]string, error) {
	return r.client.ZRangeByScore(ctx, r.queueKey, &redis.ZRangeBy{Min: "0", Max: strconv.FormatInt(endTime.UnixMilli(), 10)}).Result()
}

func (r *RedisSingleClient) RemoveFromQueue(ctx context.Context, members ...interface{}) *redis.IntCmd {
	return r.client.ZRem(ctx, r.queueKey, members...)
}

func (r *RedisSingleClient) Close() error {
	return r.client.Close()
}

func (r *RedisSingleClient) PSubscribe(ctx context.Context, channels ...string) []*redis.PubSub {
	return []*redis.PubSub{r.client.PSubscribe(ctx, channels...)}
}

type RedisClusterClient struct {
	client   *redis.ClusterClient
	queueKey string
}

func NewClusterClient(r *redis.ClusterClient, config *RedisConfig) RedisClient {
	return &RedisClusterClient{
		client:   r,
		queueKey: config.GetQueueKey(),
	}
}

func (r *RedisClusterClient) Set(ctx context.Context, key string, value interface{}, expire time.Duration) error {
	var (
		err error
		now = time.Now().Add(expire)
	)

	if err = r.client.Set(ctx, key, value, expire).Err(); err != nil {
		return err
	}

	// 队列用于补偿，这里更好的写法是使用 Lua 脚本保证事务性
	if err = r.client.ZAdd(ctx, r.queueKey, redis.Z{Member: key, Score: float64(now.UnixMilli())}).Err(); err != nil {
		return err
	}
	return nil
}

func (r *RedisClusterClient) RemoveFromQueue(ctx context.Context, members ...interface{}) *redis.IntCmd {
	return r.client.ZRem(ctx, r.queueKey, members...)
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

func (r *RedisClusterClient) QueueKey() string {
	return r.queueKey
}

func (r *RedisClusterClient) GetKeysByTime(ctx context.Context, endTime time.Time) ([]string, error) {
	return r.client.ZRangeByScore(ctx, r.queueKey, &redis.ZRangeBy{Min: "0", Max: strconv.FormatInt(endTime.UnixMilli(), 10)}).Result()
}

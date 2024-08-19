package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strconv"
	"strings"
	"sync"
	"time"
)

type EventType string

const (
	Expired EventType = "expired"
	Del     EventType = "del"
	Set     EventType = "set"
)

type Callback = func(action EventType, key string)

type Watcher struct {
	redisClient RedisClient
	queueKey    string
	jobs        []*Job
	channel     string
	pubsubs     []*redis.PubSub
	ctx         context.Context
	callback    Callback
	makeup      *Job
}

type Config struct {
	QueueKey string
	JobSize  int
	Callback Callback
}

func (config *Config) GetQueueKey() string {
	if config == nil || config.QueueKey == "" {
		return "job:delayed"
	}
	return config.QueueKey
}

func (config *Config) GetJobSize() int {
	if config == nil || config.JobSize == 0 {
		return 10
	}
	return config.JobSize
}

func (config *Config) GetCallback() Callback {
	if config == nil || config.Callback == nil {
		return func(action EventType, key string) {}
	}
	return config.Callback
}

func NewWatcher(ctx context.Context, client RedisClient, config *Config, channel string) *Watcher {
	watcher := &Watcher{
		redisClient: client,
		queueKey:    config.GetQueueKey(),
		ctx:         ctx,
		callback:    config.GetCallback(),
		channel:     channel,
	}
	pubsubs := watcher.redisClient.PSubscribe(ctx, channel)
	watcher.pubsubs = pubsubs
	for _, pubsub := range pubsubs {
		job := NewJob(func() { watcher.watchHandler(ctx, pubsub, config.GetCallback()) }, config.GetJobSize(), Immediate, false)
		watcher.jobs = append(watcher.jobs, job)
	}
	// 补偿任务
	watcher.makeup = NewJob(func() { watcher.makeUpTask() }, 1, time.Second*10, true)

	return watcher
}

func (w *Watcher) Close() error {
	for _, pubsub := range w.pubsubs {
		err := pubsub.Close()
		if err != nil {
			return err
		}
	}
	w.makeup.Close()
	for _, job := range w.jobs {
		job.Close()
	}
	return nil
}

func (w *Watcher) SetKey(ctx context.Context, key string, value interface{}, expire time.Duration) error {
	var (
		err error
		now = time.Now().Add(expire)
	)

	if err = w.redisClient.Set(ctx, key, value, expire).Err(); err != nil {
		return err
	}

	// 队列用于补偿
	if err = w.redisClient.ZAdd(ctx, w.queueKey, redis.Z{Member: key, Score: float64(now.UnixMilli())}).Err(); err != nil {
		return err
	}
	return nil
}

func (w *Watcher) Watch() {
	var wg sync.WaitGroup
	go w.makeup.Run()
	for _, job := range w.jobs {
		wg.Add(1)
		go func(job *Job) {
			job.Run()
			wg.Done()
		}(job)
	}
	wg.Wait()
}

// MakeUpTask 补偿任务
func (w *Watcher) makeUpTask() {
	// 补偿在10秒前的所有值
	var (
		timer   = time.Now().Add(-10 * time.Second)
		members []interface{}
	)
	result, err := w.redisClient.ZRangeByScore(w.ctx, w.queueKey, &redis.ZRangeBy{Min: "0", Max: strconv.FormatInt(timer.UnixMilli(), 10)}).Result()
	if err != nil {
		fmt.Printf("Watcher makeup failed: err=%v", err)
		return
	}
	if len(result) == 0 {
		return
	}
	fmt.Println(time.Now().String(), "补偿 keys:", result)
	for _, r := range result {
		w.callback(Expired, r)
		members = append(members, r)
	}

	err = w.redisClient.ZRem(w.ctx, w.queueKey, members...).Err()
	if err != nil {
		fmt.Printf("Watcher makeup failed: err=%v", err)
	}
	return
}

func (w *Watcher) watchHandler(ctx context.Context, pubsub *redis.PubSub, callback Callback) {
	msg, err := pubsub.ReceiveMessage(ctx)
	if err != nil {
		fmt.Println("Error receiving message:", err)
	}
	splitPrefix := strings.Split(w.channel, ":")
	if len(splitPrefix) == 0 {
		fmt.Println("msg unknown:", msg)
		return
	}
	eventType := strings.TrimPrefix(msg.Channel, fmt.Sprintf("%s:", splitPrefix[0]))
	callback(EventType(eventType), msg.Payload)
	// 执行成功，则删除补偿队列中的 key
	w.redisClient.ZRem(ctx, w.queueKey, msg.Payload)
}

package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strconv"
	"strings"
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
	redisClient *RedisClient
	queueKey    string
	job         *Job
	channel     string
	pubsub      *redis.PubSub
	ctx         context.Context
	callback    Callback
	makeup      *Job
}

type Config struct {
	QueueKey string
	JobSize  int
	Callback Callback
}

type QueueItem struct {
	Key   string
	Value interface{}
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

func NewWatcher(ctx context.Context, client *RedisClient, config *Config, channel string) *Watcher {
	watcher := &Watcher{
		redisClient: client,
		queueKey:    config.GetQueueKey(),
		ctx:         ctx,
		callback:    config.GetCallback(),
		channel:     channel,
	}
	pubsub := watcher.redisClient.GetClient().PSubscribe(ctx, "__keyevent@0__:*")
	watcher.pubsub = pubsub
	job := NewJob(func() { watcher.watchHandler(ctx, config.GetCallback()) }, config.GetJobSize(), Immediate, false)
	watcher.job = job

	// 补偿任务
	watcher.makeup = NewJob(func() { watcher.makeUpTask() }, 1, time.Second*10, true)

	return watcher
}

func (w *Watcher) Close() error {
	err := w.pubsub.Close()
	if err != nil {
		return err
	}
	w.makeup.Close()
	w.job.Close()
	return nil
}

func (w *Watcher) SetKey(ctx context.Context, key string, value interface{}, expire time.Duration) error {
	var (
		err error
		now = time.Now().Add(expire)
	)

	//jsonStr, err := json.Marshal(&QueueItem{Key: key, Value: value})
	//if err != nil {
	//	return err
	//}
	if err = w.redisClient.GetClient().Set(ctx, key, value, expire).Err(); err != nil {
		return err
	}

	// 队列用于补偿
	if err = w.redisClient.GetClient().ZAdd(ctx, w.queueKey, redis.Z{Member: key, Score: float64(now.UnixMilli())}).Err(); err != nil {
		return err
	}
	return nil
}

func (w *Watcher) Watch() {
	go w.makeup.Run()
	w.job.Run()
}

// MakeUpTask 补偿任务
func (w *Watcher) makeUpTask() {
	// 补偿在10秒前的所有值
	var (
		timer   = time.Now().Add(-10 * time.Second)
		members []interface{}
	)
	result, err := w.redisClient.GetClient().ZRangeByScore(w.ctx, w.queueKey, &redis.ZRangeBy{Min: "0", Max: strconv.FormatInt(timer.UnixMilli(), 10)}).Result()
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

	err = w.redisClient.GetClient().ZRem(w.ctx, w.queueKey, members...).Err()
	if err != nil {
		fmt.Printf("Watcher makeup failed: err=%v", err)
	}
	return
}

func (w *Watcher) watchHandler(ctx context.Context, callback Callback) {
	msg, err := w.pubsub.ReceiveMessage(ctx)
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
	w.redisClient.GetClient().ZRem(ctx, w.queueKey, msg.Payload)
}

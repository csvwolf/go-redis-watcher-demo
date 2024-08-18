package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strings"
)

type Watcher struct {
	redisClient *redis.Client
}

func NewWatcher(client *redis.Client) *Watcher {
	return &Watcher{
		redisClient: client,
	}
}

// 1. 需要持久化到 Redis Stream（也可以是消息队列）
// 2. 需要有补偿

func (w *Watcher) Watch(ctx context.Context) {
	pubsub := w.redisClient.PSubscribe(ctx, "__keyevent@0__:*")

	defer pubsub.Close()

	ch := make(chan struct{}, 10)

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			fmt.Println("Error receiving message:", err)
			continue
		}
		ch <- struct{}{}
		// 每次处理一个事件，启动一个新的 Goroutine
		go func(msg *redis.Message) {
			// 提取事件类型
			eventType := strings.TrimPrefix(msg.Channel, "__keyevent@0__:")

			// 根据事件类型做不同处理
			switch eventType {
			case "expired":
				fmt.Printf("Key expired: %s\n", msg.Payload)
				// 在这里处理过期事件
			case "del":
				fmt.Printf("Key deleted: %s\n", msg.Payload)
				// 在这里处理删除事件
			case "set":
				fmt.Printf("Key set: %s\n", msg.Payload)
				// 在这里处理设置事件
			default:
				fmt.Printf("Unhandled event %s for key %s\n", eventType, msg.Payload)
			}
			<-ch
		}(msg)
	}

}

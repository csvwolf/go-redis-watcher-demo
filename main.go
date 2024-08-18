package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strings"
	"sync"
)

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	ctx := context.Background()
	err := client.Set(ctx, "foo", "bar", 0).Err()
	if err != nil {
		panic(err)
	}

	pubsub := client.PSubscribe(ctx, "__keyevent@0__:*")

	defer pubsub.Close()
	var wg sync.WaitGroup
	ch := make(chan struct{}, 10)

	fmt.Println("Waiting for key events...")
	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			fmt.Println("Error receiving message:", err)
			continue
		}
		ch <- struct{}{}
		// 每次处理一个事件，启动一个新的 Goroutine
		wg.Add(1)
		go func(msg *redis.Message) {
			defer wg.Done()

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

	// 等待所有 Goroutine 执行完毕
	wg.Wait()
}

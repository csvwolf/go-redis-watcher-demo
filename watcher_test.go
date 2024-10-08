package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"math/rand"
	"testing"
	"time"
)

func TestWatcher_Watch(t *testing.T) {
	r := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	client := NewSingleClient(r, &RedisConfig{})
	ctx := context.Background()
	watcher := NewWatcher(ctx, client, &Config{
		Callback: func(action EventType, key string) {
			fmt.Println(time.Now().String(), "action:", action, "key:", key)
		},
	}, "__keyevent@0__:*")
	job := NewJob(func() {
		randSec := time.Duration(rand.Intn(10) + 1)
		randKey := fmt.Sprintf("rand_%d_%d", time.Now().Second(), randSec)
		err := client.Set(ctx, randKey, "rand", time.Second*randSec)
		if err != nil {
			fmt.Println("set key error:", err)
		}
		fmt.Println(time.Now().String(), " 生成 key:", randKey, "到期时间:", randSec)
	}, 1, time.Second*1, false)
	go job.Run()
	watcher.Watch()
	job.Close()
}

func TestWatcher_ClusterMode(t *testing.T) {
	r := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{"localhost:30001", "localhost:30002", "localhost:30003"},
	})
	client := NewClusterClient(r, &RedisConfig{})
	ctx := context.Background()
	watcher := NewWatcher(ctx, client, &Config{
		Callback: func(action EventType, key string) {
			fmt.Println(time.Now().String(), "action:", action, "key:", key)
		},
	}, "__keyevent@0__:*")
	job := NewJob(func() {
		randSec := time.Duration(rand.Intn(10) + 1)
		randKey := fmt.Sprintf("rand_%d_%d", time.Now().Second(), randSec)
		err := client.Set(ctx, randKey, "rand", time.Second*randSec)
		if err != nil {
			fmt.Println("set key error:", err)
		}
		fmt.Println(time.Now().String(), " 生成 key:", randKey, "到期时间:", randSec)
	}, 1, time.Second*1, false)
	go job.Run()
	watcher.Watch()
	job.Close()
}

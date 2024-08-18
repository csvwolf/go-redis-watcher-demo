package main

import (
	"errors"
	"fmt"
	"sync"
)

var ErrorClosedPool = errors.New("AddTask failed: pool closed")

type Task func()

type Pool struct {
	pool chan struct{}
	size int
	// 防止 pool 中的任务竞态
	mu sync.Mutex
	// 用于等待所有任务完成后关闭
	wg sync.WaitGroup
	// 用于结束 pool
	closed bool
}

// NewPool 创建一个协程池
func NewPool(size int) *Pool {
	return &Pool{
		pool: make(chan struct{}, size),
		size: size,
	}
}

// AddTask 添加协程执行任务
func (p *Pool) AddTask(task Task) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrorClosedPool
	}
	p.wg.Add(1) // 用于标记任务数量
	p.mu.Unlock()

	p.pool <- struct{}{} // 如果 pool 满了，会阻塞在这里等待
	go func() {
		// 释放任务
		defer func() {
			// 自动恢复 panic
			if r := recover(); r != nil {
				fmt.Printf("Pool run task error: %v", r)
			}
			<-p.pool
			p.wg.Done()
		}()
		task()
	}()
	return nil
}

// Close 关闭协程池
func (p *Pool) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.wg.Wait() // 等待所有任务完成
	close(p.pool)
}

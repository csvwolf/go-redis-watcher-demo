package main

import (
	"fmt"
	"sync"
	"time"
)

const Immediate = 0

type Job struct {
	task Task
	pool *Pool
	stop bool
	// 任务时间间隔
	interval time.Duration
	// panic 是否自动恢复
	autoRecover bool
	wg          sync.WaitGroup
}

// NewJob 同一个任务使用多个协程不断执行直到 Close
// interval 为 0 时，任务立即执行
func NewJob(task Task, size int, interval time.Duration, autoRecover bool) *Job {
	pool := NewPool(size, autoRecover)

	return &Job{task: task, pool: pool, interval: interval}
}

func (j *Job) Run() {
	for i := 0; i < j.pool.size; i++ {
		var ticker *time.Ticker
		if j.interval > 0 {
			ticker = time.NewTicker(j.interval)
		}
		j.wg.Add(1)
		err := j.pool.AddTask(func() {
			defer func() {
				if ticker != nil {
					ticker.Stop()
				}
				j.wg.Done()
			}()
			for !j.stop {
				if ticker != nil {
					<-ticker.C
				}
				j.task()
			}
		})
		if err != nil {
			fmt.Printf("Job Run error: %v", err)
		}
	}
	j.wg.Wait()
}

// Close 结束任务
func (j *Job) Close() {
	j.stop = true
	j.pool.Close() // 关闭池子
}

package main

import (
	"fmt"
	"time"
)

const Immediate = 0

type Job struct {
	task     Task
	pool     *Pool
	stop     bool
	interval time.Duration
}

func NewJob(task Task, size int, interval time.Duration) *Job {
	pool := NewPool(size)

	return &Job{task: task, pool: pool, interval: interval}
}

func (j *Job) Run() {
	for i := 0; i < j.pool.size; i++ {
		var ticker *time.Ticker
		if j.interval > 0 {
			ticker = time.NewTicker(j.interval)
		}
		err := j.pool.AddTask(func() {
			if ticker != nil {
				defer ticker.Stop()
			}
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
}

func (j *Job) Close() {
	j.stop = true
	j.pool.Close() // 关闭池子
}

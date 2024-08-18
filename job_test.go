package main

import (
	"testing"
	"time"
)

func TestJob_RunWithoutInterval(t *testing.T) {
	var executed int
	var start = time.Now()

	task := func() {
		t.Log("Task executed:", time.Since(start))
		executed++
		time.Sleep(800 * time.Millisecond)
	}
	job := NewJob(task, 5, Immediate, false)
	job.Run()

	time.Sleep(time.Second)
	job.Close()
	if executed != 10 { // 0, 800ms 的执行，下一轮因为 close 中断
		t.Error("Task should be executed")
	}
}

func TestJob_RunWithInterval(t *testing.T) {
	var executed int
	var start = time.Now()
	task := func() {
		t.Log("Task executed:", time.Since(start))
		executed++
	}
	job := NewJob(task, 5, 800*time.Millisecond, false)
	job.Run()

	time.Sleep(time.Second)
	job.Close()
	if executed != 10 { // 因为会等 1.6s 的任务结束
		t.Error("Task should be executed")
	}
}

func TestJob_RunBlock(t *testing.T) {
	var executed int
	var start = time.Now()

	task := func() {
		t.Log("Task executed:", time.Since(start))
		executed++
		time.Sleep(800 * time.Millisecond)
	}
	job := NewJob(task, 5, Immediate, false)
	job.Run()
}

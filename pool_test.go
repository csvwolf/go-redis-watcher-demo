package main

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool_AddTask(t *testing.T) {
	pool := NewPool(2)
	var executed int32

	task := func() {
		atomic.AddInt32(&executed, 1)
		time.Sleep(100 * time.Millisecond) // 模拟任务执行时间
	}

	// 添加多个任务到池中
	for i := 0; i < 5; i++ {
		err := pool.AddTask(task)
		if err != nil {
			t.Errorf("Failed to add task: %v", err)
		}
	}

	// 等待所有任务执行完成
	pool.Close()

	if executed != 5 {
		t.Errorf("Expected 5 tasks to be executed, but got %d", executed)
	}
}

func TestPool_Close(t *testing.T) {
	pool := NewPool(2)
	var executed int32

	task := func() {
		atomic.AddInt32(&executed, 1)
		time.Sleep(100 * time.Millisecond) // 模拟任务执行时间
	}

	// 添加几个任务
	for i := 0; i < 3; i++ {
		err := pool.AddTask(task)
		if err != nil {
			t.Errorf("Failed to add task: %v", err)
		}
	}

	// 关闭池子
	pool.Close()

	if executed != 3 {
		t.Errorf("Expected 3 tasks to be executed, but got %d", executed)
	}

	// 关闭后再添加任务应返回错误
	err := pool.AddTask(task)
	if err == nil {
		t.Error("Expected error when adding task to closed pool, but got nil")
	}
}

func TestPool_AddTaskToClosedPool(t *testing.T) {
	pool := NewPool(2)
	pool.Close() // 立即关闭池子

	task := func() {
		// 这个任务不应该被执行
		t.Error("Task should not be executed after pool is closed")
	}

	err := pool.AddTask(task)
	if !errors.Is(err, ErrorClosedPool) {
		t.Errorf("Expected pool closed error, but got %v", err)
	}
}

func TestPool_ConcurrentAddTask(t *testing.T) {
	pool := NewPool(2)
	var executed int32

	task := func() {
		atomic.AddInt32(&executed, 1)
		time.Sleep(100 * time.Millisecond) // 模拟任务执行时间
	}

	// 并发地添加任务到池中
	for i := 0; i < 10; i++ {
		go func() {
			err := pool.AddTask(task)
			if err != nil {
				t.Errorf("Failed to add task: %v", err)
			}
		}()
	}

	// 等待所有任务执行完成
	time.Sleep(1 * time.Second)
	pool.Close()

	if executed != 10 {
		t.Errorf("Expected 10 tasks to be executed, but got %d", executed)
	}
}

func TestPool_RecoveryFromPanic(t *testing.T) {
	pool := NewPool(2)
	var executed int32

	task := func() {
		atomic.AddInt32(&executed, 1)
		panic("something went wrong") // 任务中发生 panic
	}

	// 添加多个任务
	for i := 0; i < 5; i++ {
		err := pool.AddTask(task)
		if err != nil {
			t.Errorf("Failed to add task: %v", err)
		}
	}

	// 关闭池子
	pool.Close()

	// 任务执行数应该等于添加的任务数
	if executed != 5 {
		t.Errorf("Expected 5 tasks to be executed, but got %d", executed)
	}
}

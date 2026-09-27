package checkpoint

import (
	"context"
	"fmt"
	"sync"
)

// stagingBudget 是存储级共享的暂存容量预算（单位：字节）。
// 所有进行中的写入事务在内存中暂存的分片数据总量不得超过 capacity。
type stagingBudget struct {
	capacity int64
	policy   StagingPolicy

	mu     sync.Mutex
	used   int64
	notify chan struct{} // 容量释放时通知等待者（1 缓冲，只做唤醒提示）
}

func newStagingBudget(capacity int64, policy StagingPolicy) *stagingBudget {
	return &stagingBudget{
		capacity: capacity,
		policy:   policy,
		notify:   make(chan struct{}, 1),
	}
}

// acquire 申请 n 字节暂存容量。
// 拒绝策略下容量不足立即返回 ErrResourceLimit；
// 等待策略下阻塞至有容量或 ctx 取消（取消返回 ctx.Err()）。
// n 超过总容量时永远无法满足，直接返回 ErrResourceLimit。
func (b *stagingBudget) acquire(ctx context.Context, n int64) error {
	if n > b.capacity {
		return fmt.Errorf("%w: staged bytes %d exceed staging capacity %d",
			ErrResourceLimit, n, b.capacity)
	}
	for {
		b.mu.Lock()
		if b.used+n <= b.capacity {
			b.used += n
			b.mu.Unlock()
			return nil
		}
		b.mu.Unlock()
		if b.policy != StagingWait {
			return fmt.Errorf("%w: staging %d bytes would exceed capacity %d",
				ErrResourceLimit, n, b.capacity)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.notify:
		}
	}
}

// release 归还 n 字节暂存容量，并唤醒一个等待者。
func (b *stagingBudget) release(n int64) {
	b.mu.Lock()
	b.used -= n
	if b.used < 0 {
		b.used = 0
	}
	b.mu.Unlock()
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

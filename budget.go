package checkpoint

import (
	"context"
	"fmt"
	"sync"
)

// stagingBudget 是存储级共享的暂存容量预算（单位：字节）。
// 所有进行中的写入事务在内存中暂存的分片数据总量不得超过 capacity。
//
// 唤醒语义采用“代际广播”：wait 指向当前代的等待通道，一次释放若存在
// 等待者，就在持锁状态下关闭当前通道并换入新通道。关闭操作会同时唤醒
// 所有等待者，它们醒来后持锁重查容量：当前放得下的立即继续，放不下的
// 转而去等新一代通道。这样一次释放可以连续放行多个等待者，既不会丢
// 唤醒信号，也不会超发容量。
type stagingBudget struct {
	capacity int64
	policy   StagingPolicy

	mu      sync.Mutex
	used    int64
	wait    chan struct{} // 当前代等待通道；关闭即向所有等待者广播一次释放
	waiting int           // 当前阻塞在 wait 上的等待者数量
}

func newStagingBudget(capacity int64, policy StagingPolicy) *stagingBudget {
	return &stagingBudget{
		capacity: capacity,
		policy:   policy,
		wait:     make(chan struct{}),
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
		if b.policy != StagingWait {
			b.mu.Unlock()
			return fmt.Errorf("%w: staging %d bytes would exceed capacity %d",
				ErrResourceLimit, n, b.capacity)
		}
		// 持锁复制当前代通道：release 在同一把锁下关闭/替换通道，
		// 因此这里看到的通道要么尚未关闭（后续关闭必能收到广播），
		// 要么已经关闭（select 立即返回并重查容量），不存在丢唤醒窗口。
		ch := b.wait
		b.waiting++
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			b.mu.Lock()
			b.waiting--
			b.mu.Unlock()
			return ctx.Err()
		case <-ch:
			b.mu.Lock()
			b.waiting--
			b.mu.Unlock()
			// 广播唤醒可能是“虚惊”（释放出的容量被其他等待者先拿走），
			// 回到循环顶部持锁重查，放不下就继续等新一代通道。
		}
	}
}

// release 归还 n 字节暂存容量，并广播唤醒所有等待者：
// 关闭当前代等待通道后换入新通道，放得下的等待者自行继续，
// 放不下的转等新通道；不存在等待者时无需广播。
func (b *stagingBudget) release(n int64) {
	b.mu.Lock()
	b.used -= n
	if b.used < 0 {
		b.used = 0
	}
	if b.waiting > 0 {
		close(b.wait)
		b.wait = make(chan struct{})
	}
	b.mu.Unlock()
}

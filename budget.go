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

	mu      sync.Mutex
	used    int64
	waiters []*waiter // FIFO 等待队列，每个等待者持有独立的唤醒通道
}

// waiter 是等待策略下一个排队中的容量申请。
// 申请被满足时 wake 会被关闭；唤醒信号与等待者一一对应，不会丢失或串用。
type waiter struct {
	n    int64
	wake chan struct{}
}

func newStagingBudget(capacity int64, policy StagingPolicy) *stagingBudget {
	return &stagingBudget{
		capacity: capacity,
		policy:   policy,
	}
}

// acquire 申请 n 字节暂存容量。
// 拒绝策略下容量不足立即返回 ErrResourceLimit；
// 等待策略下排队阻塞至有容量或 ctx 取消（取消返回 ctx.Err()）。
// n 超过总容量时永远无法满足，直接返回 ErrResourceLimit。
func (b *stagingBudget) acquire(ctx context.Context, n int64) error {
	if n > b.capacity {
		return fmt.Errorf("%w: staged bytes %d exceed staging capacity %d",
			ErrResourceLimit, n, b.capacity)
	}
	b.mu.Lock()
	// 无排队者且当前空位足够时直接分配。队列非空时新申请一律入队尾，
	// 不得插队抢占，避免持续到达的新申请饿死先到的等待者。
	if len(b.waiters) == 0 && b.used+n <= b.capacity {
		b.used += n
		b.mu.Unlock()
		return nil
	}
	if b.policy != StagingWait {
		b.mu.Unlock()
		return fmt.Errorf("%w: staging %d bytes would exceed capacity %d",
			ErrResourceLimit, n, b.capacity)
	}
	w := &waiter{n: n, wake: make(chan struct{})}
	b.waiters = append(b.waiters, w)
	// 入队后立即尝试一次发放：可能此前已有空位只是尚未触发扫描。
	b.grantLocked()
	b.mu.Unlock()

	select {
	case <-w.wake:
		// 唤醒与取消同时发生时，取消语义优先：归还刚拿到的配额并回报取消，
		// 保证外部取消总能尽快退出且配额不泄漏。
		if err := ctx.Err(); err != nil {
			b.mu.Lock()
			b.used -= w.n
			if b.used < 0 {
				b.used = 0
			}
			b.grantLocked()
			b.mu.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		b.mu.Lock()
		// 与发放竞争：若在取消生效的同时申请已被满足，同样归还配额并继续放行
		// 其他等待者；否则从队列移除，本次申请不留任何痕迹。
		select {
		case <-w.wake:
			b.used -= w.n
			if b.used < 0 {
				b.used = 0
			}
			b.grantLocked()
		default:
			b.removeWaiterLocked(w)
		}
		b.mu.Unlock()
		return ctx.Err()
	}
}

// grantLocked 从队首开始扫描整个等待队列：凡是当前空位放得下的等待者都发放
// 配额并关闭其 wake 通道一次性放行；放不下的跳过并继续留在队列中，不会挡住
// 后面放得下的等待者。因此一次释放可以同时唤醒多个等待者，且不会超发容量。
// 调用方需持有 b.mu。
func (b *stagingBudget) grantLocked() {
	kept := make([]*waiter, 0, len(b.waiters))
	for _, w := range b.waiters {
		if b.used+w.n <= b.capacity {
			b.used += w.n
			close(w.wake)
		} else {
			kept = append(kept, w)
		}
	}
	b.waiters = kept
}

// removeWaiterLocked 从等待队列中移除指定等待者（取消路径使用）。
func (b *stagingBudget) removeWaiterLocked(target *waiter) {
	for i, w := range b.waiters {
		if w == target {
			b.waiters = append(b.waiters[:i], b.waiters[i+1:]...)
			return
		}
	}
}

// release 归还 n 字节暂存容量，并让所有当前放得下的排队等待者同时获得空位。
func (b *stagingBudget) release(n int64) {
	b.mu.Lock()
	b.used -= n
	if b.used < 0 {
		b.used = 0
	}
	// 一次释放出的空间可能足够多个等待者：全队列扫描，信号不会丢失，
	// 也不会只放行一个；具体谁能拿到由 grantLocked 按容量判定。
	b.grantLocked()
	b.mu.Unlock()
}

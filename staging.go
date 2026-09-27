package checkpoint

import (
	"context"
	"fmt"
	"sync"
)

// stagingLimiter 限制所有进行中写入事务在内存里暂存的数据总量。
// 支持两种策略：超限立即拒绝（StagingReject）或阻塞等待空位（StagingWait）。
type stagingLimiter struct {
	max    int64
	policy StagingPolicy

	mu      sync.Mutex
	used    int64
	changed chan struct{} // 每次释放后关闭并替换，用于广播唤醒等待者
}

func newStagingLimiter(max int64, policy StagingPolicy) *stagingLimiter {
	return &stagingLimiter{
		max:     max,
		policy:  policy,
		changed: make(chan struct{}),
	}
}

// acquire 申请 n 字节暂存额度。
// 拒绝策略下超限返回 ErrResourceLimit；等待策略下阻塞直至有空位或 ctx 取消。
// 单次申请超过总量上限时永远无法满足，两种策略都立即返回 ErrResourceLimit。
func (l *stagingLimiter) acquire(ctx context.Context, n int64) error {
	if n <= 0 {
		return nil
	}
	if n > l.max {
		return fmt.Errorf("%w: staged chunk %d bytes > total staging limit %d",
			ErrResourceLimit, n, l.max)
	}
	if l.policy == StagingReject {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.used+n > l.max {
			return fmt.Errorf("%w: staged %d + %d bytes > limit %d",
				ErrResourceLimit, l.used, n, l.max)
		}
		l.used += n
		return nil
	}
	for {
		l.mu.Lock()
		if l.used+n <= l.max {
			l.used += n
			l.mu.Unlock()
			return nil
		}
		ch := l.changed
		l.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// release 归还 n 字节暂存额度并唤醒所有等待者。
func (l *stagingLimiter) release(n int64) {
	if n <= 0 {
		return
	}
	l.mu.Lock()
	l.used -= n
	if l.used < 0 {
		l.used = 0
	}
	close(l.changed)
	l.changed = make(chan struct{})
	l.mu.Unlock()
}

package checkpoint

import (
	"context"
	"errors"
	"testing"
	"time"
)

// waitForBudgetWaiters 轮询预算内部等待者计数，用于在不依赖 sleep 的情况下
// 确定指定数量的等待者已经真正阻塞在容量等待上。
func waitForBudgetWaiters(t *testing.T, b *stagingBudget, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		got := b.waiting
		used := b.used
		b.mu.Unlock()
		if got == want {
			t.Logf("判定依据: 等待者数=%d used=%d/%d", got, used, b.capacity)
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("budget waiters=%d, want %d", b.snapshotWaiters(), want)
}

func (b *stagingBudget) snapshotWaiters() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.waiting
}

func (b *stagingBudget) snapshotUsed() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

func budgetResult(t *testing.T, b *stagingBudget, ctx context.Context, n int64) <-chan error {
	t.Helper()
	ch := make(chan error, 1)
	go func() { ch <- b.acquire(ctx, n) }()
	return ch
}

// 一次释放出的容量足够多个等待者时，所有当前放得下的等待者都应在同一次
// 释放后继续推进，而不是只放行一个、其余等下一次释放。
func TestStagingBudgetBroadcastWakesMultipleWaiters(t *testing.T) {
	b := newStagingBudget(10, StagingWait)
	if err := b.acquire(context.Background(), 10); err != nil {
		t.Fatalf("holder acquire: %v", err)
	}
	ctx := context.Background()
	w1 := budgetResult(t, b, ctx, 5)
	w2 := budgetResult(t, b, ctx, 5)
	waitForBudgetWaiters(t, b, 2)

	b.release(10) // 一次释放全部容量，两个等待者各需 5，都应放行

	for i, ch := range []<-chan error{w1, w2} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("waiter %d acquire after broadcast: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("waiter %d 未被同一次释放唤醒", i)
		}
	}
	if used := b.snapshotUsed(); used != 10 {
		t.Fatalf("used=%d, want 10（放行后恰好占满，不得超发）", used)
	}
	t.Logf("判定依据: 单次释放唤醒两个等待者，used=%d 未超过 capacity=10", b.snapshotUsed())
}

// 单个等待者：释放后正常放行。
func TestStagingBudgetSingleWaiterProceeds(t *testing.T) {
	b := newStagingBudget(10, StagingWait)
	if err := b.acquire(context.Background(), 7); err != nil {
		t.Fatalf("holder acquire: %v", err)
	}
	w := budgetResult(t, b, context.Background(), 4)
	waitForBudgetWaiters(t, b, 1)
	b.release(7)
	select {
	case err := <-w:
		if err != nil {
			t.Fatalf("single waiter acquire: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("single waiter not woken")
	}
	if used := b.snapshotUsed(); used != 4 {
		t.Fatalf("used=%d, want 4", used)
	}
}

// 等待中取消：即使没有任何释放发生，被取消的等待者也必须尽快返回 ctx.Err()，
// 不占容量；其取消不得影响其他等待者在后续释放时被唤醒。
func TestStagingBudgetWaiterCancellation(t *testing.T) {
	b := newStagingBudget(10, StagingWait)
	if err := b.acquire(context.Background(), 10); err != nil {
		t.Fatalf("holder acquire: %v", err)
	}
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxC, cancelC := context.WithCancel(context.Background())
	defer cancelC()
	wA := budgetResult(t, b, ctxA, 8)
	wC := budgetResult(t, b, ctxC, 2)
	waitForBudgetWaiters(t, b, 2)

	cancelA() // 不伴随任何释放
	select {
	case err := <-wA:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled waiter err=%v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("canceled waiter did not return without a release")
	}
	if used := b.snapshotUsed(); used != 10 {
		t.Fatalf("used=%d after cancel, want 10（取消不得占用容量）", used)
	}

	// 只释放 2：剩余等待者 C 需要 2，恰好放得下，应被唤醒。
	b.release(2)
	select {
	case err := <-wC:
		if err != nil {
			t.Fatalf("remaining waiter acquire: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("remaining waiter not woken after partial release")
	}
	if used := b.snapshotUsed(); used != 10 {
		t.Fatalf("used=%d, want 10（持有者 8 + C 的 2，不得超发）", used)
	}
	if waiters := b.snapshotWaiters(); waiters != 0 {
		t.Fatalf("waiting=%d, want 0（无遗留等待者）", waiters)
	}
}

// 部分释放后仍放不下的等待者必须继续等待：此时新到达且放得下的请求可以
// 立即获得空位（不超发）；等到一次足够大的释放后，原等待者最终也必须被
// 放行（不饿死、不丢已有空位）。
func TestStagingBudgetTooLargeWaiterKeepsWaiting(t *testing.T) {
	b := newStagingBudget(10, StagingWait)
	if err := b.acquire(context.Background(), 6); err != nil { // 空闲仅 4
		t.Fatalf("holder acquire: %v", err)
	}
	wA := budgetResult(t, b, context.Background(), 8) // 4 < 8，等待
	waitForBudgetWaiters(t, b, 1)

	b.release(2) // used=4，空闲 6，仍 < 8：A 被广播唤醒但必须继续等
	// A 重查后应回到等待状态（允许一个唤醒-重查的短暂窗口，轮询至稳定）。
	waitForBudgetWaiters(t, b, 1)
	select {
	case err := <-wA:
		t.Fatalf("放不下的等待者不应被放行, got err=%v", err)
	default:
	}

	// 新到达且恰好放得下（4+6=10）的请求立即成功，证明容量未被超发。
	if err := b.acquire(context.Background(), 6); err != nil {
		t.Fatalf("fitting newcomer acquire: %v", err)
	}
	if used := b.snapshotUsed(); used != 10 {
		t.Fatalf("used=%d, want 10", used)
	}
	waitForBudgetWaiters(t, b, 1) // A 仍在等
	select {
	case err := <-wA:
		t.Fatalf("A 不应在容量不足时被放行, got err=%v", err)
	default:
	}

	b.release(10) // 全部释放：A 需要的 8 终于放得下，必须被唤醒
	select {
	case err := <-wA:
		if err != nil {
			t.Fatalf("A acquire after full release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("A 在容量足够后仍被饿死")
	}
	if used := b.snapshotUsed(); used != 8 {
		t.Fatalf("used=%d, want 8", used)
	}
	if waiters := b.snapshotWaiters(); waiters != 0 {
		t.Fatalf("waiting=%d, want 0", waiters)
	}
	t.Logf("判定依据: 部分释放唤醒但不具备放行条件者继续等待，容量不超发，足量释放后最终放行")
}

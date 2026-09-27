package checkpoint

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// 暂存超限 - 拒绝策略：超限写入立即返回 ErrResourceLimit，且无残留。
func TestStagingRejectPolicy(t *testing.T) {
	s := mustOpen(t, &Config{
		MaxStagedBytes: 8,
		Staging:        StagingReject,
		MaxShardBytes:  1 << 20,
	})
	w, err := s.Begin(t.Context(), 1)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := w.WriteShard(t.Context(), 0, []byte("1234")); err != nil {
		t.Fatalf("write within budget: %v", err)
	}
	// 已暂存 4 字节，再写 5 字节超出总量 8。
	if err := w.WriteShard(t.Context(), 1, []byte("12345")); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("over-budget write err=%v, want ErrResourceLimit", err)
	}
	t.Logf("判定依据: MaxStagedBytes=8, 已暂存 4, 再写 5 -> StagingReject 立即拒绝")
	// 覆盖同序号分片：释放旧配额后重新计量，应成功。
	if err := w.WriteShard(t.Context(), 0, []byte("abcdefgh")); err != nil {
		t.Fatalf("overwrite same shard should re-budget: %v", err)
	}
	if err := w.Abort(); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if _, err := s.Latest(); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("latest err=%v, want ErrNoCheckpoint (拒绝后不得残留)", err)
	}
	// 配额已释放，可以重新写满 8 字节并提交。
	commitShards(t, s, 1, []byte("abcdefgh"))
	data, rep, err := s.Recover(1)
	if err != nil || !rep.OK() || string(data[0]) != "abcdefgh" {
		t.Fatalf("recover after release: data=%v err=%v", data, err)
	}
}

// 暂存超限 - 等待策略：空位被占时阻塞，外部取消必须尽快退出，且无残留。
func TestStagingWaitPolicyCancel(t *testing.T) {
	s := mustOpen(t, &Config{
		MaxStagedBytes: 8,
		Staging:        StagingWait,
		MaxShardBytes:  1 << 20,
		MaxInflight:    2,
	})
	// 事务 A 占满全部暂存配额。
	wa, err := s.Begin(t.Context(), 1)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	if err := wa.WriteShard(t.Context(), 0, []byte("12345678")); err != nil {
		t.Fatalf("A write: %v", err)
	}
	// 事务 B 写任意分片都会超限，进入等待。
	wb, err := s.Begin(t.Context(), 2)
	if err != nil {
		t.Fatalf("begin B: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	errCh := make(chan error, 1)
	go func() {
		errCh <- wb.WriteShard(ctx, 0, []byte("x"))
	}()
	// 确认 B 正在等待，然后取消。
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait canceled err=%v, want context.Canceled", err)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("cancel took %v, must exit promptly", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("canceled write did not return promptly")
	}
	t.Logf("判定依据: 配额被 A 占满, B 等待中 ctx 取消 -> 立即返回 context.Canceled")

	// 取消后 B 中止，不得残留；A 正常提交，配额释放。
	if err := wb.Abort(); err != nil {
		t.Fatalf("abort B: %v", err)
	}
	if _, _, err := s.Recover(2); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("recover v2 err=%v, want ErrNoCheckpoint (取消后不得残留)", err)
	}
	if err := wa.Commit(); err != nil {
		t.Fatalf("commit A: %v", err)
	}
	// A 提交后配额释放，新版本可以写入。
	commitShards(t, s, 3, []byte("ok"))
	if latest, _ := s.Latest(); latest != 3 {
		t.Fatalf("latest=%d, want 3", latest)
	}
}

// 暂存超限 - 等待策略：其他事务释放配额后，等待者应被唤醒并成功。
func TestStagingWaitPolicyUnblocked(t *testing.T) {
	s := mustOpen(t, &Config{
		MaxStagedBytes: 8,
		Staging:        StagingWait,
		MaxShardBytes:  1 << 20,
		MaxInflight:    2,
	})
	wa, err := s.Begin(t.Context(), 1)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	if err := wa.WriteShard(t.Context(), 0, []byte("12345678")); err != nil {
		t.Fatalf("A write: %v", err)
	}
	wb, err := s.Begin(t.Context(), 2)
	if err != nil {
		t.Fatalf("begin B: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var bErr error
	go func() {
		defer wg.Done()
		bErr = wb.WriteShard(context.Background(), 0, []byte("zz"))
	}()
	time.Sleep(50 * time.Millisecond) // 让 B 进入等待
	// A 提交后释放配额，B 应被唤醒。
	if err := wa.Commit(); err != nil {
		t.Fatalf("commit A: %v", err)
	}
	wg.Wait()
	if bErr != nil {
		t.Fatalf("B write after unblock: %v", bErr)
	}
	if err := wb.Commit(); err != nil {
		t.Fatalf("commit B: %v", err)
	}
	data, _, err := s.Recover(2)
	if err != nil || string(data[0]) != "zz" {
		t.Fatalf("recover v2: data=%v err=%v", data, err)
	}
	t.Logf("判定依据: A 提交释放配额 -> B 等待被唤醒并完成提交")
}

// 单笔超过总容量的写入永远无法满足，两种策略都必须立即拒绝。
func TestStagingExceedsTotalCapacity(t *testing.T) {
	for _, policy := range []StagingPolicy{StagingReject, StagingWait} {
		s := mustOpen(t, &Config{
			MaxStagedBytes: 4,
			Staging:        policy,
			MaxShardBytes:  1 << 20,
		})
		w, err := s.Begin(t.Context(), 1)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := w.WriteShard(ctx, 0, []byte("12345")); !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("policy=%d err=%v, want ErrResourceLimit", policy, err)
		}
		_ = w.Abort()
	}
}

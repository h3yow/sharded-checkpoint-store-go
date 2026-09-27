package checkpoint

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// 暂存拒绝策略：总量超限立即返回 ErrResourceLimit，失败后无残留、存储可继续用。
func TestStagingReject(t *testing.T) {
	s := mustOpen(t, &Config{MaxStagedBytes: 10, StagingPolicy: StagingReject})
	w, err := s.Begin(t.Context(), 1)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := w.WriteShard(t.Context(), 0, []byte("12345678")); err != nil {
		t.Fatalf("write within limit: %v", err)
	}
	// 8 + 4 > 10，超限必须拒绝。
	if err := w.WriteShard(t.Context(), 1, []byte("1234")); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("over-limit write err=%v, want ErrResourceLimit", err)
	}
	// 单个分片超过总量上限，永远无法满足，立即拒绝。
	if err := w.WriteShard(t.Context(), 1, []byte("12345678901")); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("never-fits write err=%v, want ErrResourceLimit", err)
	}
	// 覆盖已有分片按差额记账：8 -> 10 字节应成功。
	if err := w.WriteShard(t.Context(), 0, []byte("0123456789")); err != nil {
		t.Fatalf("overwrite within limit: %v", err)
	}
	if err := w.Abort(); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if _, err := s.Latest(); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("latest err=%v, want ErrNoCheckpoint (拒绝后无残留)", err)
	}
	// 中止后额度已释放，可以重新写满并提交。
	commitShards(t, s, 1, []byte("0123456789"))
	data, _, err := s.Recover(1)
	if err != nil || string(data[0]) != "0123456789" {
		t.Fatalf("recover after abort: data=%q err=%v", data[0], err)
	}
	t.Logf("判定依据: MaxStagedBytes=10, 超限写入被 ErrResourceLimit 拒绝, Abort 后额度释放可正常提交")
}

// 暂存等待策略：超限阻塞等待空位；外部取消必须尽快退出且不留残留。
func TestStagingWaitCancel(t *testing.T) {
	s := mustOpen(t, &Config{MaxStagedBytes: 10, StagingPolicy: StagingWait, MaxInflight: 2})
	// 事务 A 占满全部暂存额度。
	wa, err := s.Begin(t.Context(), 1)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	if err := wa.WriteShard(t.Context(), 0, []byte("0123456789")); err != nil {
		t.Fatalf("A write: %v", err)
	}
	// 事务 B 的写入必然阻塞。
	wb, err := s.Begin(t.Context(), 2)
	if err != nil {
		t.Fatalf("begin B: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- wb.WriteShard(ctx, 0, []byte("abcd"))
	}()
	// 确认 B 确实在等待，然后取消。
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("B write err=%v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled write did not return promptly")
	}
	t.Logf("判定依据: 等待中的写入在 ctx 取消后立即返回 context.Canceled")
	// 取消不占用额度：B 中止，A 正常提交。
	if err := wb.Abort(); err != nil {
		t.Fatalf("abort B: %v", err)
	}
	if err := wa.Commit(); err != nil {
		t.Fatalf("commit A: %v", err)
	}
	if _, err := os.Stat(s.chkDir(2)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled writer leaves residue dir chk-2")
	}
	if _, _, err := s.Recover(2); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("recover v2 err=%v, want ErrNoCheckpoint (取消后无残留)", err)
	}
	if latest, _ := s.Latest(); latest != 1 {
		t.Fatalf("latest=%d, want 1", latest)
	}
}

// 暂存等待策略：有其他事务释放额度后，等待者应被放行继续。
func TestStagingWaitUnblocked(t *testing.T) {
	s := mustOpen(t, &Config{MaxStagedBytes: 10, StagingPolicy: StagingWait, MaxInflight: 2})
	wa, err := s.Begin(t.Context(), 1)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	if err := wa.WriteShard(t.Context(), 0, []byte("0123456789")); err != nil {
		t.Fatalf("A write: %v", err)
	}
	wb, err := s.Begin(t.Context(), 2)
	if err != nil {
		t.Fatalf("begin B: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- wb.WriteShard(context.Background(), 0, []byte("abcd"))
	}()
	time.Sleep(50 * time.Millisecond) // 确认 B 在等待
	// A 提交后释放额度，B 应被放行。
	if err := wa.Commit(); err != nil {
		t.Fatalf("commit A: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("B write after unblock: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiting write not unblocked after A committed")
	}
	if err := wb.Commit(); err != nil {
		t.Fatalf("commit B: %v", err)
	}
	data, _, err := s.Recover(2)
	if err != nil || string(data[0]) != "abcd" {
		t.Fatalf("recover v2: data=%q err=%v", data[0], err)
	}
	t.Logf("判定依据: A 提交释放暂存额度后, B 的等待写入被放行并成功提交")
}

// 等待策略下，单个分片超过总量上限永远无法满足，必须立即拒绝而非死等。
func TestStagingWaitNeverFits(t *testing.T) {
	s := mustOpen(t, &Config{MaxStagedBytes: 4, StagingPolicy: StagingWait})
	w, err := s.Begin(t.Context(), 1)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := w.WriteShard(t.Context(), 0, []byte("12345")); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("err=%v, want ErrResourceLimit", err)
	}
	_ = w.Abort()
}

package checkpoint

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMaxShardsReject(t *testing.T) {
	s := openStore(t, Options{MaxShards: 2, Policy: Reject})
	w, err := s.Begin(context.Background(), 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	for i := 0; i < 2; i++ {
		if err := w.WriteShard(i, strings.NewReader("x"), 1); err != nil {
			t.Fatalf("WriteShard(%d): %v", i, err)
		}
	}
	err = w.WriteShard(2, strings.NewReader("x"), 1)
	t.Logf("input: MaxShards=2, 写第 3 个分片 -> err=%v", err)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("err=%v, want ErrLimitExceeded", err)
	}
}

func TestMaxShardBytesReject(t *testing.T) {
	s := openStore(t, Options{MaxShardBytes: 4, Policy: Reject})
	w, _ := s.Begin(context.Background(), 1, 0)
	defer w.Abort()
	err := w.WriteShard(0, strings.NewReader("12345"), 5)
	t.Logf("input: MaxShardBytes=4, 写 5 字节分片 -> err=%v", err)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("err=%v, want ErrLimitExceeded", err)
	}
	// 声明 size=-1（未知长度）时按实际写入字节数判定。
	w2, _ := s.Begin(context.Background(), 2, 0)
	defer w2.Abort()
	err = w2.WriteShard(0, strings.NewReader("12345"), -1)
	t.Logf("input: MaxShardBytes=4, 未知长度写 5 字节 -> err=%v", err)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("err=%v, want ErrLimitExceeded", err)
	}
}

func TestMaxBufferedBytesReject(t *testing.T) {
	s := openStore(t, Options{MaxBufferedBytes: 8, Policy: Reject})
	w, _ := s.Begin(context.Background(), 1, 0)
	defer w.Abort()
	if err := w.WriteShard(0, strings.NewReader("12345"), 5); err != nil {
		t.Fatal(err)
	}
	err := w.WriteShard(1, strings.NewReader("12345"), 5)
	t.Logf("input: MaxBufferedBytes=8, 累计缓冲 10 字节 -> err=%v", err)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("err=%v, want ErrLimitExceeded", err)
	}
}

func TestLimitFailureLeavesNoValidCheckpoint(t *testing.T) {
	s := openStore(t, Options{MaxShards: 1, Policy: Reject})
	w, _ := s.Begin(context.Background(), 1, 0)
	w.WriteShard(0, strings.NewReader("ok"), 2)
	if err := w.WriteShard(1, strings.NewReader("overflow"), 8); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("err=%v, want ErrLimitExceeded", err)
	}
	// 即使调用方不 Abort 直接丢弃，staging 也不得被误认为有效检查点。
	s2, err := Open(s.dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s2.Latest()
	t.Logf("decision: 超限失败后 Latest err=%v", err)
	if !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("Latest err=%v, want ErrNoCheckpoint（不得残留有效检查点）", err)
	}
	if _, _, err := s2.Recover(1); !errors.Is(err, ErrIncompleteCommit) {
		t.Fatalf("Recover(1) err=%v, want ErrIncompleteCommit", err)
	}
}

func TestMaxInflightCommitsReject(t *testing.T) {
	s := openStore(t, Options{MaxInflightCommits: 1, Policy: Reject})
	w1, err := s.Begin(context.Background(), 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w1.Abort()
	_, err = s.Begin(context.Background(), 2, 0)
	t.Logf("input: MaxInflightCommits=1, 第二个并发会话 -> err=%v", err)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("err=%v, want ErrLimitExceeded", err)
	}
	// 第一个会话结束后配额释放。
	if err := w1.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Begin(context.Background(), 2, 0); err != nil {
		t.Fatalf("Begin after release: %v", err)
	}
}

func TestMaxInflightCommitsBlock(t *testing.T) {
	s := openStore(t, Options{MaxInflightCommits: 1, Policy: Block})
	w1, _ := s.Begin(context.Background(), 1, 0)

	got := make(chan error, 1)
	go func() {
		w2, err := s.Begin(context.Background(), 2, 0)
		if err == nil {
			got <- w2.Abort()
			return
		}
		got <- err
	}()
	select {
	case err := <-got:
		t.Fatalf("Block 策略下第二个 Begin 应立即阻塞，却返回 %v", err)
	case <-time.After(50 * time.Millisecond):
		t.Log("decision: 第二个 Begin 按 Block 策略阻塞中")
	}
	w1.Abort() // 释放配额
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("blocked Begin err=%v", err)
		}
		t.Log("decision: 配额释放后阻塞的 Begin 成功获取")
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Begin did not proceed after release")
	}
}

func TestBlockPolicyContextCancel(t *testing.T) {
	s := openStore(t, Options{MaxInflightCommits: 1, Policy: Block})
	w1, _ := s.Begin(context.Background(), 1, 0)
	defer w1.Abort()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := s.Begin(ctx, 2, 0)
	t.Logf("input: Block 策略 + ctx 超时 -> err=%v", err)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want context.DeadlineExceeded", err)
	}
}

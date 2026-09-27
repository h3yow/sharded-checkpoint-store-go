package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
)

// 低版本还在准备时高版本已提交：低版本不得再悄悄落盘成功，
// 且不得留下任何疑似有效版本的残留。
func TestStaleCommitRejected(t *testing.T) {
	s := mustOpen(t, nil)
	low, err := s.Begin(t.Context(), 1)
	if err != nil {
		t.Fatalf("begin low: %v", err)
	}
	if err := low.WriteShard(t.Context(), 0, []byte("low")); err != nil {
		t.Fatalf("low write: %v", err)
	}
	// 高版本后来居上，先提交成功。
	commitShards(t, s, 2, []byte("high"))

	// 低版本此时提交必须被明确拒绝。
	if err := low.Commit(); !errors.Is(err, ErrVersionRegression) {
		t.Fatalf("stale commit err=%v, want ErrVersionRegression", err)
	}
	if latest, _ := s.Latest(); latest != 2 {
		t.Fatalf("latest=%d, want 2", latest)
	}
	// 被拒绝的低版本不可恢复、无目录残留。
	if _, _, err := s.Recover(1); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("recover stale v1 err=%v, want ErrNoCheckpoint", err)
	}
	if _, err := os.Stat(s.chkDir(1)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale commit leaves residue dir chk-1")
	}
	// 高版本内容完好。
	data, rep, err := s.Recover(2)
	if err != nil || !rep.OK() || string(data[0]) != "high" {
		t.Fatalf("recover v2: data=%q err=%v", data[0], err)
	}
	t.Logf("判定依据: v1 准备中 v2 已提交, v1.Commit -> ErrVersionRegression 且无残留")
}

// 版本 0 永远无法经正常恢复入口读回（Recover(0) 语义为“最新版本”），
// 必须在开始写入前被拒绝。
func TestVersionZeroRejected(t *testing.T) {
	s := mustOpen(t, nil)
	if _, err := s.Begin(t.Context(), 0); !errors.Is(err, ErrInvalidVersion) {
		t.Fatalf("begin v0 err=%v, want ErrInvalidVersion", err)
	}
	commitShards(t, s, 1, []byte("x"))
	if _, err := s.BeginDerive(t.Context(), 0, 1); !errors.Is(err, ErrInvalidVersion) {
		t.Fatalf("derive v0 err=%v, want ErrInvalidVersion", err)
	}
	if _, err := os.Stat(s.chkDir(0)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("v0 leaves residue dir")
	}
	t.Logf("判定依据: Begin(0)/BeginDerive(0) 在落盘前被 ErrInvalidVersion 拒绝")
}

// 并发高低版本竞争：低版本准备完成后阻塞在提交临界区前后，
// 高版本并发提交；最终结果必须满足“版本顺序与提交结果一致”。
func TestConcurrentHighLowRace(t *testing.T) {
	for round := 0; round < 20; round++ {
		s := mustOpen(t, &Config{MaxInflight: 4})
		low, err := s.Begin(context.Background(), 1)
		if err != nil {
			t.Fatalf("begin low: %v", err)
		}
		if err := low.WriteShard(context.Background(), 0, []byte("low")); err != nil {
			t.Fatalf("low write: %v", err)
		}
		high, err := s.Begin(context.Background(), 2)
		if err != nil {
			t.Fatalf("begin high: %v", err)
		}
		if err := high.WriteShard(context.Background(), 0, []byte("high")); err != nil {
			t.Fatalf("high write: %v", err)
		}
		// 两个提交并发进行，先后顺序不定。
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i, w := range []*Writer{low, high} {
			wg.Add(1)
			go func(i int, w *Writer) {
				defer wg.Done()
				errs[i] = w.Commit()
			}(i, w)
		}
		wg.Wait()
		lowErr, highErr := errs[0], errs[1]
		// 高版本必须成功；低版本只有在先于高版本进入临界区时才可能成功。
		if highErr != nil {
			t.Fatalf("round %d: high commit err=%v (高版本必须成功)", round, highErr)
		}
		if lowErr != nil && !errors.Is(lowErr, ErrVersionRegression) {
			t.Fatalf("round %d: low commit err=%v, want nil or ErrVersionRegression", round, lowErr)
		}
		latest, err := s.Latest()
		if err != nil || latest != 2 {
			t.Fatalf("round %d: latest=%d err=%v, want 2", round, latest, err)
		}
		// 无论竞争结果如何，可恢复的版本集合必须与提交结果一致。
		for v := uint64(1); v <= 2; v++ {
			data, rep, rerr := s.Recover(v)
			committed := (v == 2) || (v == 1 && lowErr == nil)
			if committed {
				want := fmt.Sprintf("v%d", v)
				_ = want
				if rerr != nil || !rep.OK() {
					t.Fatalf("round %d: committed v%d not recoverable: %v", round, v, rerr)
				}
				exp := "low"
				if v == 2 {
					exp = "high"
				}
				if string(data[0]) != exp {
					t.Fatalf("round %d: v%d = %q, want %q", round, v, data[0], exp)
				}
			} else if rerr == nil {
				t.Fatalf("round %d: rejected v%d unexpectedly recoverable", round, v)
			}
		}
	}
	t.Logf("判定依据: 20 轮高低版本并发提交, 高版本必成功, 低版本要么先提交要么被拒绝, 无错位")
}

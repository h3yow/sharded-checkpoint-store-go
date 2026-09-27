package checkpoint

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// 高低版本竞争：低版本还在准备时高版本已提交，低版本不得再落盘成功。
func TestLowVersionCommitRejectedAfterHighCommits(t *testing.T) {
	s := mustOpen(t, nil)
	commitShards(t, s, 1, []byte("base"))

	// 低版本 v2 先开始准备。
	w2, err := s.Begin(t.Context(), 2)
	if err != nil {
		t.Fatalf("begin v2: %v", err)
	}
	if err := w2.WriteShard(t.Context(), 0, []byte("v2")); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	// 高版本 v3 后开始但先提交成功。
	commitShards(t, s, 3, []byte("v3"))

	// 低版本 v2 的提交必须被明确拒绝。
	if err := w2.Commit(); !errors.Is(err, ErrVersionRegression) {
		t.Fatalf("commit v2 err=%v, want ErrVersionRegression", err)
	}
	t.Logf("判定依据: v3 已提交, v2 提交时版本准入复核 -> ErrVersionRegression")

	// 存储里不能出现版本顺序和提交结果对不上的内容。
	if _, _, err := s.Recover(2); err == nil {
		t.Fatalf("rejected v2 must not be recoverable")
	}
	if latest, _ := s.Latest(); latest != 3 {
		t.Fatalf("latest=%d, want 3", latest)
	}
	data, rep, err := s.Recover(0)
	if err != nil || !rep.OK() || string(data[0]) != "v3" {
		t.Fatalf("recover latest: data=%v err=%v", data, err)
	}
	// v2 被拒绝后版本号可重新使用。
	commitShards(t, s, 4, []byte("v4"))
}

// 并发提交同一批乱序版本：最终存储状态必须与提交结果一致。
func TestConcurrentOutOfOrderCommits(t *testing.T) {
	s := mustOpen(t, &Config{MaxInflight: 8})
	// v1..v4 全部先开始准备。
	writers := make([]*Writer, 5)
	for v := uint64(1); v <= 4; v++ {
		w, err := s.Begin(context.Background(), v)
		if err != nil {
			t.Fatalf("begin v%d: %v", v, err)
		}
		if err := w.WriteShard(context.Background(), 0, []byte{byte('0' + v)}); err != nil {
			t.Fatalf("write v%d: %v", v, err)
		}
		writers[v] = w
	}
	// 按 3,1,4,2 的乱序并发提交。
	order := []uint64{3, 1, 4, 2}
	results := make(map[uint64]error, 4)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, v := range order {
		wg.Add(1)
		go func(v uint64) {
			defer wg.Done()
			err := writers[v].Commit()
			mu.Lock()
			results[v] = err
			mu.Unlock()
		}(v)
	}
	wg.Wait()
	// 不变式：报告成功的版本必须可恢复且内容正确；报告失败的版本必须不可恢复。
	for v := uint64(1); v <= 4; v++ {
		err := results[v]
		data, _, rerr := s.Recover(v)
		if err == nil {
			if rerr != nil || len(data) == 0 || data[0][0] != byte('0'+v) {
				t.Fatalf("v%d committed but recover: data=%v err=%v", v, data, rerr)
			}
		} else {
			if !errors.Is(err, ErrVersionRegression) {
				t.Fatalf("v%d commit err=%v, want ErrVersionRegression", v, err)
			}
			if rerr == nil {
				t.Fatalf("v%d rejected but recoverable (提交结果与存储状态不一致)", v)
			}
		}
	}
	// 最高版本必定成功，Latest 必须等于它。
	if results[4] != nil {
		t.Fatalf("v4 (highest) must commit: %v", results[4])
	}
	if latest, _ := s.Latest(); latest != 4 {
		t.Fatalf("latest=%d, want 4", latest)
	}
	t.Logf("判定依据: 乱序并发提交结果=%v, 成功者可恢复/失败者不可恢复, latest=4", results)
}

// 版本 0 被 Recover(0)=最新版本 的语义占用，提交后将无法按编号读回，
// 必须在开始写入前拒绝，且不得产生任何残留。
func TestVersionZeroRejected(t *testing.T) {
	s := mustOpen(t, nil)
	if _, err := s.Begin(t.Context(), 0); !errors.Is(err, ErrInvalidVersion) {
		t.Fatalf("begin v0 err=%v, want ErrInvalidVersion", err)
	}
	if _, err := s.BeginFrom(t.Context(), 0, 1); !errors.Is(err, ErrInvalidVersion) {
		t.Fatalf("beginFrom v0 err=%v, want ErrInvalidVersion", err)
	}
	// 不得占用事务槽位：随后正常版本可立即开始。
	w, err := s.Begin(t.Context(), 1)
	if err != nil {
		t.Fatalf("begin v1 after rejected v0: %v", err)
	}
	_ = w.Abort()
}

// 重复提交与覆盖已提交版本在准入阶段即被拒绝（含派生入口）。
func TestDuplicateAndOverwriteRejected(t *testing.T) {
	s := mustOpen(t, nil)
	commitShards(t, s, 5, []byte("v5"))
	if _, err := s.Begin(t.Context(), 5); !errors.Is(err, ErrVersionExists) {
		t.Fatalf("begin v5 err=%v, want ErrVersionExists", err)
	}
	if _, err := s.BeginFrom(t.Context(), 5, 5); !errors.Is(err, ErrVersionExists) {
		t.Fatalf("beginFrom v5 err=%v, want ErrVersionExists", err)
	}
	if _, err := s.Begin(t.Context(), 4); !errors.Is(err, ErrVersionRegression) {
		t.Fatalf("begin v4 err=%v, want ErrVersionRegression", err)
	}
	if _, err := s.BeginFrom(t.Context(), 4, 5); !errors.Is(err, ErrVersionRegression) {
		t.Fatalf("beginFrom v4 err=%v, want ErrVersionRegression", err)
	}
	// 原有内容不受影响。
	data, rep, err := s.Recover(5)
	if err != nil || !rep.OK() || string(data[0]) != "v5" {
		t.Fatalf("recover v5: data=%v err=%v", data, err)
	}
}

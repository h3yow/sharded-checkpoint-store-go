package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// TestConcurrentCommitsDistinctVersions 多 goroutine 并发提交不同版本。
// 严格单调语义下，提交时落后于 latest 的版本会被拒绝（ErrVersionRegression）；
// 不变量：无论成功与否，最终每个已提交版本都必须完整可恢复、内容无错位。
func TestConcurrentCommitsDistinctVersions(t *testing.T) {
	s := openStore(t, Options{})
	const n = 16
	t.Logf("input: %d 个 goroutine 并发提交 version=1..%d（乱序到达）", n, n)

	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(v uint64) {
			defer wg.Done()
			w, err := s.Begin(context.Background(), v, 0)
			if err != nil {
				if errors.Is(err, ErrVersionExists) || errors.Is(err, ErrVersionRegression) {
					return // 合法拒绝
				}
				errs <- fmt.Errorf("begin v%d: %w", v, err)
				return
			}
			payload := []byte(fmt.Sprintf("payload-of-v%d", v))
			if err := w.WriteShard(0, bytes.NewReader(payload), int64(len(payload))); err != nil {
				errs <- fmt.Errorf("write v%d: %w", v, err)
				return
			}
			if err := w.Commit(); err != nil {
				if errors.Is(err, ErrVersionExists) || errors.Is(err, ErrVersionRegression) {
					return // 合法拒绝：提交时已有更高版本
				}
				errs <- fmt.Errorf("commit v%d: %w", v, err)
			}
		}(uint64(i))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	versions, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("decision: committed versions=%v（被拒绝的均为回退版本）", versions)
	if len(versions) == 0 {
		t.Fatal("no version committed")
	}
	// 已提交版本必须严格递增（List 保证），且每个都完整、内容无错位。
	for i, v := range versions {
		if i > 0 && v <= versions[i-1] {
			t.Fatalf("versions not strictly increasing: %v", versions)
		}
		if _, report, err := s.Recover(v); err != nil || !report.Complete {
			t.Fatalf("Recover(%d): err=%v complete=%v", v, err, report.Complete)
		}
		got, err := s.ReadShard(v, 0)
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("payload-of-v%d", v)
		if string(got) != want {
			t.Fatalf("v%d shard0=%q, want %q（分片错位）", v, got, want)
		}
	}
	latest, _ := s.Latest()
	if latest != versions[len(versions)-1] {
		t.Fatalf("Latest=%d, want %d", latest, versions[len(versions)-1])
	}
}

// TestConcurrentCommitSameVersion N 个会话竞争同一版本，恰好一个成功。
func TestConcurrentCommitSameVersion(t *testing.T) {
	s := openStore(t, Options{})
	const n = 8
	t.Logf("input: %d 个 goroutine 竞争提交同一 version=1", n)

	var wg sync.WaitGroup
	var okCount, dupCount int64
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			w, err := s.Begin(context.Background(), 1, 0)
			if err != nil {
				if errors.Is(err, ErrVersionExists) {
					mu.Lock()
					dupCount++
					mu.Unlock()
				}
				return
			}
			payload := fmt.Sprintf("writer-%d", i)
			w.WriteShard(0, strings.NewReader(payload), int64(len(payload)))
			if err := w.Commit(); err != nil {
				if errors.Is(err, ErrVersionExists) {
					mu.Lock()
					dupCount++
					mu.Unlock()
				} else {
					t.Errorf("unexpected commit err: %v", err)
				}
				return
			}
			mu.Lock()
			okCount++
			mu.Unlock()
		}(i)
	}
	close(start)
	wg.Wait()
	t.Logf("decision: committed=%d rejected=%d", okCount, dupCount)
	if okCount != 1 {
		t.Fatalf("okCount=%d, want exactly 1", okCount)
	}
	if _, report, err := s.Recover(1); err != nil || !report.Complete {
		t.Fatalf("final state broken: err=%v report=%+v", err, report)
	}
}

// TestRecoverDuringConcurrentWrites 恢复与写入并行时，
// 恢复只能看到完整提交的检查点，不得读到中间态。
func TestRecoverDuringConcurrentWrites(t *testing.T) {
	s := openStore(t, Options{})
	commitShards(t, s, 1, 0, [][]byte{[]byte("stable-v1")})

	stop := make(chan struct{})
	var writerWG, recoverWG sync.WaitGroup
	// 写入者：持续提交新版本，直到 stop 关闭。
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for v := uint64(2); ; v++ {
			select {
			case <-stop:
				return
			default:
			}
			w, err := s.Begin(context.Background(), v, 0)
			if err != nil {
				continue
			}
			p := []byte(fmt.Sprintf("data-v%d", v))
			if w.WriteShard(0, bytes.NewReader(p), int64(len(p))) == nil {
				w.Commit()
			} else {
				w.Abort()
			}
		}
	}()
	// 恢复者：不断恢复最新版本，任何一次都必须完整。
	recoverWG.Add(1)
	go func() {
		defer recoverWG.Done()
		for i := 0; i < 200; i++ {
			_, report, err := s.Recover(0)
			if errors.Is(err, ErrNoCheckpoint) {
				continue
			}
			if err != nil {
				t.Errorf("Recover during writes: %v", err)
				return
			}
			if !report.Complete {
				t.Errorf("read intermediate state: %+v", report.Failures)
				return
			}
		}
	}()
	recoverWG.Wait() // 先等恢复者跑完固定轮次
	close(stop)      // 再通知写入者退出，避免互相等待死锁
	writerWG.Wait()
	t.Log("decision: 200 次并发恢复全部看到完整检查点，无中间态")
}

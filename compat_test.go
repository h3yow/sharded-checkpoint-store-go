package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// 并发提交：多个 goroutine 并行提交不同版本，最终状态必须可预期。
func TestConcurrentCommits(t *testing.T) {
	s := mustOpen(t, &Config{MaxInflight: 16})
	const n = 16
	// 第一阶段：全部 Begin 并发进行。
	writers := make([]*Writer, n)
	var wg sync.WaitGroup
	beginErrs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(v uint64) {
			defer wg.Done()
			w, err := s.Begin(context.Background(), v)
			if err == nil {
				for sh := 0; sh < 3; sh++ {
					err = w.WriteShard(t.Context(), sh, []byte(fmt.Sprintf("v%d-s%d", v, sh)))
					if err != nil {
						break
					}
				}
			}
			writers[v-1], beginErrs[v-1] = w, err
		}(uint64(i + 1))
	}
	wg.Wait()
	for i, err := range beginErrs {
		if err != nil {
			t.Fatalf("begin/write v%d: %v", i+1, err)
		}
	}
	// 第二阶段：全部 Commit 并发进行。
	// 严格版本准入下，提交被串行化且只允许单调递增：
	// 成功集合必为“提交时创新高”的版本子序列，最高版本 n 必定成功；
	// 在高版本之后落盘的低版本必须被明确拒绝且不留可读残留。
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(v uint64) {
			defer wg.Done()
			errs[v-1] = writers[v-1].Commit()
		}(uint64(i + 1))
	}
	wg.Wait()
	if errs[n-1] != nil {
		t.Fatalf("commit highest v%d: %v (最高版本必须成功)", n, errs[n-1])
	}
	var okVersions []uint64
	for v := uint64(1); v <= n; v++ {
		err := errs[v-1]
		if err == nil {
			okVersions = append(okVersions, v)
			continue
		}
		// 失败的只能是“低版本在高版本之后落盘”，必须报版本回退。
		if !errors.Is(err, ErrVersionRegression) {
			t.Fatalf("commit v%d err=%v, want ErrVersionRegression", v, err)
		}
		// 被拒绝的版本不得留下可读回的有效内容。
		if _, _, rerr := s.Recover(v); rerr == nil {
			t.Fatalf("rejected v%d must not be recoverable", v)
		}
		if _, statErr := os.Stat(s.chkDir(v)); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("rejected v%d leaves residue dir", v)
		}
	}
	// 成功集合必须严格递增（版本顺序与提交结果一致）。
	for i := 1; i < len(okVersions); i++ {
		if okVersions[i] <= okVersions[i-1] {
			t.Fatalf("committed versions not increasing: %v", okVersions)
		}
	}
	latest, err := s.Latest()
	if err != nil || latest != n {
		t.Fatalf("latest=%d err=%v, want %d", latest, err, n)
	}
	// 每个成功提交的版本都必须完整可恢复，无半写入可见。
	for _, v := range okVersions {
		data, rep, err := s.Recover(v)
		if err != nil || !rep.OK() {
			t.Fatalf("recover v%d: err=%v rep=%+v", v, err, rep)
		}
		for sh := 0; sh < 3; sh++ {
			want := fmt.Sprintf("v%d-s%d", v, sh)
			if string(data[sh]) != want {
				t.Fatalf("v%d shard %d = %q, want %q (分片错位)", v, sh, data[sh], want)
			}
		}
	}
	t.Logf("判定依据: 成功提交集合 %v 严格递增且 latest=%d, 被拒绝版本不可恢复且无残留", okVersions, latest)
}

// 并发恢复与提交交错：读取者不得看到中间态。
func TestConcurrentRecoverDuringCommit(t *testing.T) {
	s := mustOpen(t, nil)
	commitShards(t, s, 1, []byte("stable"))
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				data, rep, err := s.Recover(0) // 最新已提交版本
				if err != nil {
					t.Errorf("recover latest: %v", err)
					return
				}
				if !rep.OK() || string(data[0]) != "stable" && string(data[0]) != "v2" {
					t.Errorf("read intermediate state: %q", data[0])
					return
				}
			}
		}()
	}
	w, _ := s.Begin(context.Background(), 2)
	_ = w.WriteShard(t.Context(), 0, []byte("v2"))
	_ = w.Commit()
	close(stop)
	wg.Wait()
}

// 并发写入同一版本：第二个 Begin 必须被拒绝。
func TestConcurrentSameVersionRejected(t *testing.T) {
	s := mustOpen(t, nil)
	w1, err := s.Begin(t.Context(), 7)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := s.Begin(t.Context(), 7); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("second begin same version err=%v, want ErrResourceLimit", err)
	}
	_ = w1.Abort()
	// Abort 后该版本可重新写入。
	w2, err := s.Begin(t.Context(), 7)
	if err != nil {
		t.Fatalf("begin after abort: %v", err)
	}
	_ = w2.Abort()
}

// 格式兼容：v1 清单（无 CRC32 字段）可读；超出支持范围必须拒绝。
func TestFormatCompatibility(t *testing.T) {
	s := mustOpen(t, nil)
	dir := s.chkDir(1)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// 手写 v1 检查点：分片无清单级 CRC32。
	payload := []byte("legacy-v1")
	enc := encodeShard(1, 0, payload)
	if err := os.WriteFile(filepath.Join(dir, "shard-00000.ckpt"), enc, 0o644); err != nil {
		t.Fatal(err)
	}
	v1Manifest := []byte(`{"format_version":1,"version":1,"shard_count":1,` +
		fmt.Sprintf(`"shards":[{"index":0,"size":%d,"chunks":1}]}`, len(enc)))
	if err := os.WriteFile(filepath.Join(dir, manifestName), v1Manifest, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 手写 v1 清单(无 crc32 字段)=%s", v1Manifest)

	data, rep, err := s.Recover(1)
	if err != nil || !rep.OK() {
		t.Fatalf("recover v1: err=%v rep=%+v", err, rep)
	}
	if string(data[0]) != string(payload) {
		t.Fatalf("v1 payload=%q, want %q", data[0], payload)
	}
	if rep.Format != 1 {
		t.Fatalf("report format=%d, want 1", rep.Format)
	}
	t.Logf("判定依据: v1 缺失 crc32 字段 -> 缺省为 0, 退化为分片内部校验, 恢复结论确定")

	// 超出支持范围的格式版本必须明确拒绝。
	dir2 := s.chkDir(2)
	os.MkdirAll(dir2, 0o755)
	os.WriteFile(filepath.Join(dir2, manifestName),
		[]byte(`{"format_version":99,"version":2,"shard_count":0,"shards":[]}`), 0o644)
	if _, _, err := s.Recover(2); !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("recover v99 err=%v, want ErrUnsupportedFormat", err)
	}
	t.Logf("判定依据: format=99 超出 [%d,%d] -> ErrUnsupportedFormat", MinFormatVersion, MaxFormatVersion)
}

// 清单本身损坏必须报损坏而非静默跳过。
func TestCorruptManifest(t *testing.T) {
	s := mustOpen(t, nil)
	commitShards(t, s, 1, []byte("x"))
	p := filepath.Join(s.chkDir(1), manifestName)
	os.WriteFile(p, []byte("{not json"), 0o644)
	if _, _, err := s.Recover(1); !errors.Is(err, ErrShardCorrupted) {
		t.Fatalf("err=%v, want ErrShardCorrupted", err)
	}
}

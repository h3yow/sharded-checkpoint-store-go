package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func mustOpen(t *testing.T, cfg *Config) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return s
}

func commitShards(t *testing.T, s *Store, version uint64, payloads ...[]byte) {
	t.Helper()
	w, err := s.Begin(t.Context(), version)
	if err != nil {
		t.Fatalf("begin v%d: %v", version, err)
	}
	for i, p := range payloads {
		if err := w.WriteShard(t.Context(), i, p); err != nil {
			t.Fatalf("write shard %d: %v", i, err)
		}
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("commit v%d: %v", version, err)
	}
}

// 中断写入：未提交的检查点不得被当作最新版本加载。
func TestInterruptedWriteNotVisible(t *testing.T) {
	s := mustOpen(t, nil)
	commitShards(t, s, 1, []byte("a0"), []byte("a1"))

	// 模拟进程中断：写了部分分片但从未 Commit，也不 Abort。
	w, err := s.Begin(t.Context(), 2)
	if err != nil {
		t.Fatalf("begin v2: %v", err)
	}
	if err := w.WriteShard(t.Context(), 0, []byte("b0")); err != nil {
		t.Fatalf("write shard: %v", err)
	}
	t.Logf("中断场景: version=2 写入 1/3 分片后中断, 输入 shard0=%q", "b0")

	// 最新版本仍应是 1。
	latest, err := s.Latest()
	if err != nil || latest != 1 {
		t.Fatalf("latest=%d err=%v, want 1", latest, err)
	}
	// 直接恢复 v2 必须报未完整提交。
	_, _, err = s.Recover(2)
	if !errors.Is(err, ErrIncompleteCommit) {
		t.Fatalf("recover v2 err=%v, want ErrIncompleteCommit", err)
	}
	t.Logf("判定依据: v2 目录存在但无 manifest.json -> ErrIncompleteCommit")

	// 重新打开存储（模拟重启），结论不变。
	s2, err := Open(s.dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if latest, _ := s2.Latest(); latest != 1 {
		t.Fatalf("after reopen latest=%d, want 1", latest)
	}
	if _, _, err := s2.Recover(2); !errors.Is(err, ErrIncompleteCommit) {
		t.Fatalf("after reopen recover v2 err=%v, want ErrIncompleteCommit", err)
	}
}

// Abort 后不得残留可被误认为有效的检查点。
func TestAbortLeavesNoResidue(t *testing.T) {
	s := mustOpen(t, nil)
	w, err := s.Begin(t.Context(), 5)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_ = w.WriteShard(t.Context(), 0, []byte("x"))
	if err := w.Abort(); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if _, err := os.Stat(s.chkDir(5)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("chk dir still exists after abort")
	}
	if _, err := s.Latest(); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("latest err=%v, want ErrNoCheckpoint", err)
	}
}

// 分片损坏：缺失、截断、损坏需给出可区分原因。
func TestShardIntegrityFailures(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(dir string)
		want   error
	}{
		{"missing", func(dir string) {
			os.Remove(filepath.Join(dir, "shard-00001.ckpt"))
		}, ErrShardMissing},
		{"truncated", func(dir string) {
			p := filepath.Join(dir, "shard-00001.ckpt")
			data, _ := os.ReadFile(p)
			os.WriteFile(p, data[:len(data)-3], 0o644)
		}, ErrShardTruncated},
		{"corrupted", func(dir string) {
			p := filepath.Join(dir, "shard-00001.ckpt")
			data, _ := os.ReadFile(p)
			data[shardHeaderLen] ^= 0xFF // 翻转 payload 首字节
			os.WriteFile(p, data, 0o644)
		}, ErrShardCorrupted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustOpen(t, nil)
			commitShards(t, s, 1, []byte("s0"), []byte("s1"), []byte("s2"))
			tc.mutate(s.chkDir(1))
			t.Logf("输入: version=1 shards=3, 故障注入=%s", tc.name)
			_, rep, err := s.Recover(1)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err=%v, want %v", err, tc.want)
			}
			if rep == nil || rep.OK() {
				t.Fatalf("report should mark failure: %+v", rep)
			}
			for _, sr := range rep.Shards {
				t.Logf("shard=%d ok=%v err=%v", sr.Index, sr.OK, sr.Err)
			}
			// 失败分片原因必须可区分，其余分片必须正常。
			if !errors.Is(rep.Shards[1].Err, tc.want) {
				t.Fatalf("shard1 report err=%v, want %v", rep.Shards[1].Err, tc.want)
			}
			if !rep.Shards[0].OK || !rep.Shards[2].OK {
				t.Fatalf("healthy shards must report OK: %+v", rep.Shards)
			}
		})
	}
}

// 版本规则：重复提交、覆盖已提交版本、版本回退均被拒绝。
func TestVersionRules(t *testing.T) {
	s := mustOpen(t, nil)
	commitShards(t, s, 3, []byte("v3"))

	if _, err := s.Begin(t.Context(), 3); !errors.Is(err, ErrVersionExists) {
		t.Fatalf("re-commit v3 err=%v, want ErrVersionExists", err)
	}
	if _, err := s.Begin(t.Context(), 2); !errors.Is(err, ErrVersionRegression) {
		t.Fatalf("regress to v2 err=%v, want ErrVersionRegression", err)
	}
	t.Logf("判定依据: committed=3, begin(3)->ErrVersionExists, begin(2)->ErrVersionRegression")

	// 追加新版本仍然允许，且旧版本仍可恢复。
	commitShards(t, s, 4, []byte("v4"))
	data, rep, err := s.Recover(3)
	if err != nil || !rep.OK() || string(data[0]) != "v3" {
		t.Fatalf("recover old v3: data=%v err=%v", data, err)
	}
	if latest, _ := s.Latest(); latest != 4 {
		t.Fatalf("latest=%d, want 4", latest)
	}
}

// 资源上限：分片数与分片大小超限必须拒绝。
func TestResourceLimits(t *testing.T) {
	s := mustOpen(t, &Config{MaxShards: 2, MaxShardBytes: 4, MaxInflight: 1})
	w, err := s.Begin(t.Context(), 1)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := w.WriteShard(t.Context(), 2, []byte("ab")); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("shard index 2 err=%v, want ErrResourceLimit", err)
	}
	if err := w.WriteShard(t.Context(), 0, []byte("12345")); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("oversize shard err=%v, want ErrResourceLimit", err)
	}
	t.Logf("判定依据: MaxShards=2, MaxShardBytes=4, 超限写入均被 ErrResourceLimit 拒绝")
	if err := w.Abort(); err != nil {
		t.Fatalf("abort: %v", err)
	}
	// 失败后不得残留。
	if _, err := s.Latest(); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("latest err=%v, want ErrNoCheckpoint", err)
	}
}

// 空提交与分片不连续必须失败。
func TestCommitValidation(t *testing.T) {
	s := mustOpen(t, nil)
	w, _ := s.Begin(t.Context(), 1)
	if err := w.Commit(); err == nil {
		t.Fatalf("commit with no shards should fail")
	}
	w2, _ := s.Begin(t.Context(), 1)
	_ = w2.WriteShard(t.Context(), 1, []byte("gap")) // 跳过 index 0
	if err := w2.Commit(); err == nil {
		t.Fatalf("commit with non-contiguous shards should fail")
	}
	fmt.Println("commit validation ok")
}

package checkpoint

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func openStore(t *testing.T, opt Options) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "ckpt"), opt)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

// commitShards 提交一个包含 n 个分片的检查点，返回各分片内容。
func commitShards(t *testing.T, s *Store, version, base uint64, payloads [][]byte) {
	t.Helper()
	w, err := s.Begin(t.Context(), version, base)
	if err != nil {
		t.Fatalf("Begin(v=%d): %v", version, err)
	}
	for i, p := range payloads {
		if err := w.WriteShard(i, bytes.NewReader(p), int64(len(p))); err != nil {
			t.Fatalf("WriteShard(%d): %v", i, err)
		}
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("Commit(v=%d): %v", version, err)
	}
}

func TestCommitAndRecoverRoundtrip(t *testing.T) {
	s := openStore(t, Options{})
	payloads := [][]byte{[]byte("shard-zero"), []byte("shard-one"), bytes.Repeat([]byte("x"), 4096)}
	t.Logf("input: version=1 shards=%d sizes=%v", len(payloads), sizesOf(payloads))
	commitShards(t, s, 1, 0, payloads)

	latest, err := s.Latest()
	if err != nil || latest != 1 {
		t.Fatalf("Latest=%d err=%v, want 1", latest, err)
	}
	m, report, err := s.Recover(0) // 0 = 最新已提交
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	t.Logf("recovered: version=%d format=%d complete=%v", m.Version, m.Format, report.Complete)
	if m.Version != 1 || !report.Complete || len(m.Shards) != 3 {
		t.Fatalf("unexpected manifest/report: %+v %+v", m, report)
	}
	for i, want := range payloads {
		got, err := s.ReadShard(1, i)
		if err != nil {
			t.Fatalf("ReadShard(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("shard %d content mismatch", i)
		}
	}
}

func TestInterruptedWriteNotVisible(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ckpt")
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	commitShards(t, s, 1, 0, [][]byte{[]byte("committed")})

	// 模拟进程中断：写入会话未 Commit 也未 Abort，staging 残留。
	w, err := s.Begin(t.Context(), 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteShard(0, strings.NewReader("partial-data"), 12); err != nil {
		t.Fatal(err)
	}
	t.Logf("input: 中断的写入 version=2 已写 1 分片, staging=%s", w.stagingPath)
	// 不 Commit、不 Abort，直接丢弃（模拟崩溃）。

	// 重新打开存储（模拟进程重启），v2 不得被当作最新版本。
	s2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	latest, err := s2.Latest()
	t.Logf("recover decision: latest=%d err=%v (staging 目录不参与版本判定)", latest, err)
	if err != nil || latest != 1 {
		t.Fatalf("Latest=%d err=%v, want 1（中断的 v2 不可见）", latest, err)
	}
	if _, _, err := s2.Recover(2); !errors.Is(err, ErrIncompleteCommit) {
		t.Fatalf("Recover(2) err=%v, want ErrIncompleteCommit", err)
	}
}

func TestAbortLeavesNoResidue(t *testing.T) {
	s := openStore(t, Options{})
	w, err := s.Begin(t.Context(), 5, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteShard(0, strings.NewReader("data"), 4); err != nil {
		t.Fatal(err)
	}
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(w.stagingPath); !os.IsNotExist(err) {
		t.Fatalf("staging dir still exists after Abort: %v", err)
	}
	if _, err := s.Latest(); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("Latest err=%v, want ErrNoCheckpoint", err)
	}
	// Abort 后同一版本可重新提交。
	commitShards(t, s, 5, 0, [][]byte{[]byte("retry")})
}

func TestShardFailuresAreDistinguishable(t *testing.T) {
	cases := []struct {
		name   string
		corrupt func(dir string)
		want   ShardFailureReason
	}{
		{"missing", func(dir string) {
			os.Remove(filepath.Join(dir, shardsDir, "000001.bin"))
		}, ShardMissing},
		{"truncated", func(dir string) {
			os.WriteFile(filepath.Join(dir, shardsDir, "000001.bin"), []byte("xx"), 0o644)
		}, ShardTruncated},
		{"corrupted", func(dir string) {
			p := filepath.Join(dir, shardsDir, "000001.bin")
			orig, _ := os.ReadFile(p)
			mut := bytes.Clone(orig)
			for i := range mut {
				mut[i] ^= 0xff
			}
			os.WriteFile(p, mut, 0o644) // 长度不变、内容破坏
		}, ShardCorrupted},
		{"orphan", func(dir string) {
			os.WriteFile(filepath.Join(dir, shardsDir, "000099.bin"), []byte("stray"), 0o644)
		}, ShardOrphan},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := openStore(t, Options{})
			payloads := [][]byte{[]byte("aaaa"), []byte("bbbbbbbb"), []byte("cc")}
			commitShards(t, s, 1, 0, payloads)
			dir := s.committedPath(1)
			tc.corrupt(dir)
			t.Logf("input: 对 version=1 注入 %s 故障", tc.name)

			_, report, err := s.Recover(1)
			if !errors.Is(err, ErrShardIntegrity) {
				t.Fatalf("err=%v, want ErrShardIntegrity", err)
			}
			if report.Complete {
				t.Fatal("report.Complete=true, want false")
			}
			var reasons []ShardFailureReason
			for _, f := range report.Failures {
				reasons = append(reasons, f.Reason)
			}
			t.Logf("report: failures=%v", reasons)
			if len(report.Failures) == 0 || report.Failures[0].Reason != tc.want {
				t.Fatalf("failures=%+v, want first reason %s", report.Failures, tc.want)
			}
		})
	}
}

func TestAppendNewVersionFromBase(t *testing.T) {
	s := openStore(t, Options{})
	commitShards(t, s, 1, 0, [][]byte{[]byte("base-0"), []byte("base-1")})
	t.Logf("input: 基于 v1 追加 1 个分片生成 v2")

	w, err := s.Begin(t.Context(), 2, 1)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := w.WriteShard(2, strings.NewReader("appended"), 8); err != nil {
		t.Fatalf("WriteShard(2): %v", err)
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	m, report, err := s.Recover(2)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	t.Logf("recovered: version=%d source=%d shards=%d complete=%v",
		m.Version, m.SourceVersion, len(m.Shards), report.Complete)
	if m.SourceVersion != 1 || len(m.Shards) != 3 || !report.Complete {
		t.Fatalf("unexpected: %+v %+v", m, report)
	}
	got, _ := s.ReadShard(2, 0)
	if string(got) != "base-0" {
		t.Fatalf("shard 0 = %q, want base-0（基线分片应保留）", got)
	}
	got, _ = s.ReadShard(2, 2)
	if string(got) != "appended" {
		t.Fatalf("shard 2 = %q, want appended", got)
	}
}

func TestVersionConflictsRejected(t *testing.T) {
	s := openStore(t, Options{})
	commitShards(t, s, 3, 0, [][]byte{[]byte("v3")})

	t.Run("duplicate", func(t *testing.T) {
		_, err := s.Begin(t.Context(), 3, 0)
		t.Logf("input: 重复提交 version=3 -> err=%v", err)
		if !errors.Is(err, ErrVersionExists) {
			t.Fatalf("err=%v, want ErrVersionExists", err)
		}
	})
	t.Run("regression", func(t *testing.T) {
		_, err := s.Begin(t.Context(), 2, 0)
		t.Logf("input: 版本回退 version=2 (latest=3) -> err=%v", err)
		if !errors.Is(err, ErrVersionRegression) {
			t.Fatalf("err=%v, want ErrVersionRegression", err)
		}
	})
	t.Run("overwrite via racing commit", func(t *testing.T) {
		// 两个会话同时 Begin 同一新版本，先提交者成功，后提交者必须被拒绝。
		w1, err := s.Begin(t.Context(), 4, 0)
		if err != nil {
			t.Fatal(err)
		}
		w2, err := s.Begin(t.Context(), 4, 0)
		if err != nil {
			t.Fatal(err)
		}
		w1.WriteShard(0, strings.NewReader("first"), 5)
		w2.WriteShard(0, strings.NewReader("second"), 6)
		if err := w1.Commit(); err != nil {
			t.Fatalf("w1.Commit: %v", err)
		}
		err = w2.Commit()
		t.Logf("input: 竞争提交 version=4, 后提交者 err=%v", err)
		if !errors.Is(err, ErrVersionExists) {
			t.Fatalf("err=%v, want ErrVersionExists", err)
		}
		// 失败提交不得残留：v4 内容必须是 w1 的。
		got, err := s.ReadShard(4, 0)
		if err != nil || string(got) != "first" {
			t.Fatalf("v4 shard0=%q err=%v, want first", got, err)
		}
	})
}

func sizesOf(payloads [][]byte) string {
	var parts []string
	for _, p := range payloads {
		parts = append(parts, fmt.Sprint(len(p)))
	}
	return strings.Join(parts, ",")
}

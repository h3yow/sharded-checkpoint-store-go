package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// 版本承接：派生版本的恢复结果必须与“旧版本全量重写 + 同样改动”一致，
// 且来源版本不受影响。
func TestDeriveEqualsFullRewrite(t *testing.T) {
	s := mustOpen(t, nil)
	commitShards(t, s, 1, []byte("a0"), []byte("a1"), []byte("a2"))

	// 派生 v2：承接 v1，仅改动 shard 1。
	w, err := s.BeginDerive(t.Context(), 2, 1)
	if err != nil {
		t.Fatalf("begin derive: %v", err)
	}
	if err := w.WriteShard(t.Context(), 1, []byte("b1")); err != nil {
		t.Fatalf("write shard: %v", err)
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("commit derive: %v", err)
	}

	// 对照组：v3 全量重写 v1 内容并做同样改动。
	commitShards(t, s, 3, []byte("a0"), []byte("b1"), []byte("a2"))

	derived, rep, err := s.Recover(2)
	if err != nil || !rep.OK() {
		t.Fatalf("recover derived v2: err=%v rep=%+v", err, rep)
	}
	rewritten, _, err := s.Recover(3)
	if err != nil {
		t.Fatalf("recover rewritten v3: %v", err)
	}
	if len(derived) != len(rewritten) {
		t.Fatalf("shard count %d != %d", len(derived), len(rewritten))
	}
	for i := range rewritten {
		if string(derived[i]) != string(rewritten[i]) {
			t.Fatalf("shard %d: derived %q != rewritten %q", i, derived[i], rewritten[i])
		}
	}
	t.Logf("判定依据: 派生 v2 恢复结果 %q 与全量重写 v3 完全一致",
		[]string{string(derived[0]), string(derived[1]), string(derived[2])})

	// 来源版本 v1 必须保持原样。
	base, rep, err := s.Recover(1)
	if err != nil || !rep.OK() {
		t.Fatalf("recover source v1: err=%v", err)
	}
	for i, want := range []string{"a0", "a1", "a2"} {
		if string(base[i]) != want {
			t.Fatalf("source v1 shard %d = %q, want %q (来源被改动)", i, base[i], want)
		}
	}
}

// 来源不可用：不存在、未完整提交、分片损坏，派生都必须在落任何新数据前失败，
// 且不留下疑似有效新版本的残留。
func TestDeriveSourceUnavailable(t *testing.T) {
	t.Run("not-found", func(t *testing.T) {
		s := mustOpen(t, nil)
		commitShards(t, s, 1, []byte("x"))
		_, err := s.BeginDerive(t.Context(), 2, 9)
		if !errors.Is(err, ErrNoCheckpoint) {
			t.Fatalf("err=%v, want ErrNoCheckpoint", err)
		}
		assertNoDeriveResidue(t, s, 2, 1)
	})

	t.Run("incomplete", func(t *testing.T) {
		s := mustOpen(t, nil)
		commitShards(t, s, 1, []byte("x"))
		// 模拟中断：v5 有目录但从未提交。
		w, err := s.Begin(t.Context(), 5)
		if err != nil {
			t.Fatalf("begin v5: %v", err)
		}
		if err := w.WriteShard(t.Context(), 0, []byte("partial")); err != nil {
			t.Fatalf("write: %v", err)
		}
		_, err = s.BeginDerive(t.Context(), 6, 5)
		if !errors.Is(err, ErrIncompleteCommit) {
			t.Fatalf("err=%v, want ErrIncompleteCommit", err)
		}
		assertNoDeriveResidue(t, s, 6, 1)
		_ = w.Abort()
	})

	t.Run("corrupted", func(t *testing.T) {
		s := mustOpen(t, nil)
		commitShards(t, s, 1, []byte("x"))
		// 损坏来源分片。
		p := filepath.Join(s.chkDir(1), "shard-00000.ckpt")
		data, _ := os.ReadFile(p)
		data[shardHeaderLen] ^= 0xFF
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := s.BeginDerive(t.Context(), 2, 1)
		if !errors.Is(err, ErrShardCorrupted) {
			t.Fatalf("err=%v, want ErrShardCorrupted", err)
		}
		// 来源不可读时 committed 仍为 1（Latest 不依赖内容完整），只需无新版本残留。
		if _, statErr := os.Stat(s.chkDir(2)); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("derive left residue dir chk-2")
		}
	})
}

func assertNoDeriveResidue(t *testing.T, s *Store, version, wantLatest uint64) {
	t.Helper()
	if _, err := os.Stat(s.chkDir(version)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("derive left residue dir chk-%d", version)
	}
	if _, _, err := s.Recover(version); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("recover v%d err=%v, want ErrNoCheckpoint", version, err)
	}
	if latest, err := s.Latest(); err != nil || latest != wantLatest {
		t.Fatalf("latest=%d err=%v, want %d", latest, err, wantLatest)
	}
	// 失败后同一版本号必须还能重新开始（pending 已释放）。
	w, err := s.Begin(t.Context(), version)
	if err != nil {
		t.Fatalf("begin v%d after failed derive: %v", version, err)
	}
	_ = w.Abort()
}

// 来源为较老格式版本（v1 清单，无 crc32 字段）：派生读取结果必须确定，
// 与从 v2 来源派生得到的内容一致。
func TestDeriveFromLegacyFormat(t *testing.T) {
	s := mustOpen(t, nil)
	// 手写 v1 检查点作为来源。
	dir := s.chkDir(1)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
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

	w, err := s.BeginDerive(t.Context(), 2, 1)
	if err != nil {
		t.Fatalf("derive from v1: %v", err)
	}
	if err := w.WriteShard(t.Context(), 1, []byte("new-shard")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	data, rep, err := s.Recover(2)
	if err != nil || !rep.OK() {
		t.Fatalf("recover derived: err=%v rep=%+v", err, rep)
	}
	if string(data[0]) != "legacy-v1" || string(data[1]) != "new-shard" {
		t.Fatalf("derived = %q,%q; want legacy-v1,new-shard", data[0], data[1])
	}
	if rep.Format != MaxFormatVersion {
		t.Fatalf("derived format=%d, want %d (派生输出总是当前格式)", rep.Format, MaxFormatVersion)
	}
	t.Logf("判定依据: v1 来源缺失 crc32 字段按缺省规则确定读取, 派生 v2=%q 且以 v%d 格式落盘",
		[]string{string(data[0]), string(data[1])}, rep.Format)
}

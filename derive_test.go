package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// 派生提交：恢复结果必须等于“承接内容 + 本次改动”，与全量重写一致；来源版本不受影响。
func TestDeriveMatchesFullRewrite(t *testing.T) {
	s := mustOpen(t, nil)
	commitShards(t, s, 1, []byte("a0"), []byte("a1"), []byte("a2"))

	// 派生 v2：承接 v1，仅改动 shard1，并追加 shard3。
	w, err := s.BeginFrom(t.Context(), 2, 1)
	if err != nil {
		t.Fatalf("begin from: %v", err)
	}
	if err := w.WriteShard(t.Context(), 1, []byte("b1")); err != nil {
		t.Fatalf("write shard: %v", err)
	}
	if err := w.WriteShard(t.Context(), 3, []byte("b3")); err != nil {
		t.Fatalf("write shard: %v", err)
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// 对照组：全量重写 v3，内容 = v1 承接 + 同样的改动。
	commitShards(t, s, 3, []byte("a0"), []byte("b1"), []byte("a2"), []byte("b3"))

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
	t.Logf("判定依据: 派生 v2 与全量重写 v3 逐分片一致: %v", derived)

	// 来源版本 v1 不受影响。
	base, rep, err := s.Recover(1)
	if err != nil || !rep.OK() {
		t.Fatalf("recover base v1: err=%v", err)
	}
	for i, want := range []string{"a0", "a1", "a2"} {
		if string(base[i]) != want {
			t.Fatalf("base v1 shard %d = %q, want %q (来源被污染)", i, base[i], want)
		}
	}
}

// 来源不可用：不存在 / 未完整提交 / 分片损坏，派生必须在落任何新数据前失败且无残留。
func TestDeriveFromUnavailableBase(t *testing.T) {
	t.Run("not-exist", func(t *testing.T) {
		s := mustOpen(t, nil)
		if _, err := s.BeginFrom(t.Context(), 2, 9); !errors.Is(err, ErrNoCheckpoint) {
			t.Fatalf("err=%v, want ErrNoCheckpoint", err)
		}
		assertNoResidue(t, s, 2)
	})

	t.Run("incomplete-commit", func(t *testing.T) {
		s := mustOpen(t, nil)
		// 模拟中断：v1 写了分片但从未提交。
		w, err := s.Begin(t.Context(), 1)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if err := w.WriteShard(t.Context(), 0, []byte("x")); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, err := s.BeginFrom(t.Context(), 2, 1); !errors.Is(err, ErrIncompleteCommit) {
			t.Fatalf("err=%v, want ErrIncompleteCommit", err)
		}
		assertNoResidue(t, s, 2)
	})

	t.Run("corrupt-shard", func(t *testing.T) {
		s := mustOpen(t, nil)
		commitShards(t, s, 1, []byte("s0"), []byte("s1"))
		// 损坏来源的一个分片。
		p := filepath.Join(s.chkDir(1), "shard-00001.ckpt")
		data, _ := os.ReadFile(p)
		data[shardHeaderLen] ^= 0xFF
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := s.BeginFrom(t.Context(), 2, 1); !errors.Is(err, ErrShardCorrupted) {
			t.Fatalf("err=%v, want ErrShardCorrupted", err)
		}
		assertNoResidue(t, s, 2)
	})
}

// assertNoResidue 断言指定版本不存在任何看似有效的内容。
func assertNoResidue(t *testing.T, s *Store, version uint64) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(s.chkDir(version), manifestName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("v%d has manifest after failed derive", version)
	}
	if _, err := os.Stat(s.chkDir(version)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("v%d chk dir left after failed derive", version)
	}
	if _, _, err := s.Recover(version); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("recover v%d err=%v, want ErrNoCheckpoint", version, err)
	}
	// 失败后该版本仍可重新使用。
	w, err := s.Begin(t.Context(), version)
	if err != nil {
		t.Fatalf("begin v%d after failed derive: %v", version, err)
	}
	_ = w.Abort()
}

// 来源为较老格式版本（v1 清单无 CRC32 字段）：派生读取结果必须确定。
func TestDeriveFromLegacyFormat(t *testing.T) {
	s := mustOpen(t, nil)
	// 手写 v1 格式检查点作为来源。
	dir := s.chkDir(1)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	payloads := [][]byte{[]byte("legacy-0"), []byte("legacy-1")}
	var shards string
	for i, p := range payloads {
		enc := encodeShard(1, i, p)
		name := fmt.Sprintf("shard-%05d.ckpt", i)
		if err := os.WriteFile(filepath.Join(dir, name), enc, 0o644); err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			shards += ","
		}
		shards += fmt.Sprintf(`{"index":%d,"size":%d,"chunks":1}`, i, len(enc))
	}
	v1Manifest := []byte(`{"format_version":1,"version":1,"shard_count":2,"shards":[` + shards + `]}`)
	if err := os.WriteFile(filepath.Join(dir, manifestName), v1Manifest, 0o644); err != nil {
		t.Fatal(err)
	}

	w, err := s.BeginFrom(t.Context(), 2, 1)
	if err != nil {
		t.Fatalf("begin from legacy v1: %v", err)
	}
	if err := w.WriteShard(t.Context(), 0, []byte("new-0")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	data, rep, err := s.Recover(2)
	if err != nil || !rep.OK() {
		t.Fatalf("recover derived: err=%v rep=%+v", err, rep)
	}
	if string(data[0]) != "new-0" || string(data[1]) != "legacy-1" {
		t.Fatalf("derived = %v, want [new-0 legacy-1]", data)
	}
	if rep.Format != MaxFormatVersion {
		t.Fatalf("derived format=%d, want %d (派生版本应以当前格式落盘)", rep.Format, MaxFormatVersion)
	}
	t.Logf("判定依据: v1 来源缺省 crc32=0 确定性读取, 派生 v2 以 v%d 格式落盘, 内容=%v",
		MaxFormatVersion, data)
}

// 派生事务中止：不得影响来源版本，也不留残留。
func TestDeriveAbort(t *testing.T) {
	s := mustOpen(t, nil)
	commitShards(t, s, 1, []byte("a0"))
	w, err := s.BeginFrom(t.Context(), 2, 1)
	if err != nil {
		t.Fatalf("begin from: %v", err)
	}
	if err := w.Abort(); err != nil {
		t.Fatalf("abort: %v", err)
	}
	assertNoResidue(t, s, 2)
	data, _, err := s.Recover(1)
	if err != nil || string(data[0]) != "a0" {
		t.Fatalf("base v1 after abort: data=%v err=%v", data, err)
	}
}

package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// writeRawManifest 直接在已提交目录中放置手写清单与分片，用于格式兼容测试。
func writeRawManifest(t *testing.T, s *Store, version uint64, manifest string, shards map[string][]byte) {
	t.Helper()
	dir := s.committedPath(version)
	if err := os.MkdirAll(filepath.Join(dir, shardsDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, manifestFile), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	for name, data := range shards {
		if err := os.WriteFile(filepath.Join(dir, shardsDir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFormatV1ReadableWithDeterministicDefaults(t *testing.T) {
	s := openStore(t, Options{})
	// V1 清单：无 source_version / created_at 字段。
	payload := []byte("v1-shard-data")
	manifest := fmt.Sprintf(`{
  "format": 1,
  "version": 7,
  "shards": [{"index": 0, "size": %d, "sha256": "%s"}]
}`, len(payload), sha256HexOf(payload))
	// 注意：sha256HexOf 返回 hex 字符串，清单中应原样写入（%s）。
	t.Logf("input: 手写 V1 清单 version=7, 缺失 source_version/created_at 字段")
	writeRawManifest(t, s, 7, manifest, map[string][]byte{"000000.bin": payload})

	m, report, err := s.Recover(7)
	if err != nil {
		t.Fatalf("Recover(V1): %v", err)
	}
	t.Logf("recovered: format=%d source=%d created_at_zero=%v complete=%v",
		m.Format, m.SourceVersion, m.CreatedAt.IsZero(), report.Complete)
	if m.Format != FormatV1 {
		t.Fatalf("format=%d, want V1", m.Format)
	}
	// 缺失字段按确定默认值处理：SourceVersion=0（全新检查点语义）。
	if m.SourceVersion != 0 || !m.CreatedAt.IsZero() {
		t.Fatalf("V1 defaults not deterministic: %+v", m)
	}
	if !report.Complete {
		t.Fatalf("report incomplete: %+v", report.Failures)
	}
	got, err := s.ReadShard(7, 0)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("ReadShard=%q err=%v", got, err)
	}
}

func TestUnsupportedFormatRejected(t *testing.T) {
	cases := []struct {
		name    string
		format  int
		wantErr error
	}{
		{"too_old", 0, ErrUnsupportedFormat},
		{"too_new", 99, ErrUnsupportedFormat},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := openStore(t, Options{})
			manifest := fmt.Sprintf(`{"format": %d, "version": 1, "shards": []}`, tc.format)
			t.Logf("input: format=%d, 支持范围=[%d,%d]", tc.format, MinSupportedFormat, CurrentFormat)
			writeRawManifest(t, s, 1, manifest, nil)
			_, _, err := s.Recover(1)
			t.Logf("decision: err=%v", err)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err=%v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestManifestIndexGapRejected(t *testing.T) {
	s := openStore(t, Options{})
	manifest := `{"format": 2, "version": 1, "shards": [{"index": 1, "size": 0, "sha256": ""}]}`
	t.Logf("input: 清单分片索引从 1 开始（存在缺口）")
	writeRawManifest(t, s, 1, manifest, nil)
	if _, _, err := s.Recover(1); err == nil {
		t.Fatal("expected error for index gap, got nil")
	} else {
		t.Logf("decision: err=%v", err)
	}
}

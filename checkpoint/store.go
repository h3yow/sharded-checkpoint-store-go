package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	committedDir   = "committed"
	stagingDir     = "staging"
	shardsDir      = "shards"
	manifestFile   = "manifest.json"
	versionPrefix  = "v"
	versionDigits  = 20
	shardNameFmt   = "%06d.bin"
	maxShardReadUp = 1 << 62 // 防御性上限，避免清单声明超大尺寸时内存暴涨
)

// Store 本地分片检查点存储，并发安全。
//
// 磁盘布局：
//
//	<dir>/committed/v<version>/manifest.json   已提交检查点（原子发布）
//	<dir>/committed/v<version>/shards/NNNNNN.bin
//	<dir>/staging/w-<id>/...                   写入中的临时区，恢复时不可见
type Store struct {
	dir string
	opt Options

	mu        sync.Mutex // 保护版本判定与提交临界区
	commitSem chan struct{}
}

// Open 打开（必要时创建）位于 dir 的检查点存储。
func Open(dir string, opt Options) (*Store, error) {
	if opt.MaxInflightCommits < 0 || opt.MaxShards < 0 || opt.MaxShardBytes < 0 || opt.MaxBufferedBytes < 0 {
		return nil, fmt.Errorf("checkpoint: negative limit in options")
	}
	for _, sub := range []string{committedDir, stagingDir} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, fmt.Errorf("checkpoint: create %s: %w", sub, err)
		}
	}
	s := &Store{dir: dir, opt: opt}
	if opt.MaxInflightCommits > 0 {
		s.commitSem = make(chan struct{}, opt.MaxInflightCommits)
	}
	return s, nil
}

func (s *Store) committedPath(version uint64) string {
	return filepath.Join(s.dir, committedDir, versionDirName(version))
}

func versionDirName(version uint64) string {
	return versionPrefix + fmt.Sprintf("%0*d", versionDigits, version)
}

func parseVersionDir(name string) (uint64, bool) {
	if !strings.HasPrefix(name, versionPrefix) || len(name) != 1+versionDigits {
		return 0, false
	}
	v, err := strconv.ParseUint(name[1:], 10, 64)
	return v, err == nil
}

// acquireCommitSlot 按策略获取提交配额。
func (s *Store) acquireCommitSlot(ctx context.Context) error {
	if s.commitSem == nil {
		return nil
	}
	if s.opt.Policy == Block {
		select {
		case s.commitSem <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case s.commitSem <- struct{}{}:
		return nil
	default:
		return fmt.Errorf("%w: inflight commits limit %d", ErrLimitExceeded, s.opt.MaxInflightCommits)
	}
}

func (s *Store) releaseCommitSlot() {
	if s.commitSem == nil {
		return
	}
	select {
	case <-s.commitSem:
	default:
	}
}

// checkVersionLocked 在持锁状态下判定 version 是否可提交。
func (s *Store) checkVersionLocked(version uint64) error {
	versions, err := s.listLocked()
	if err != nil {
		return err
	}
	for _, v := range versions {
		if v == version {
			return fmt.Errorf("%w: version=%d", ErrVersionExists, version)
		}
	}
	if len(versions) > 0 && version < versions[len(versions)-1] {
		return fmt.Errorf("%w: version=%d latest=%d", ErrVersionRegression, version, versions[len(versions)-1])
	}
	return nil
}

// Begin 开始一个写入会话，目标版本为 version。
// 基于 base 版本追加时传 base>0（base 必须已提交），全新检查点传 0。
func (s *Store) Begin(ctx context.Context, version uint64, base uint64) (*Writer, error) {
	if version == 0 {
		return nil, fmt.Errorf("checkpoint: version must be >= 1")
	}
	if err := s.acquireCommitSlot(ctx); err != nil {
		return nil, err
	}
	w := &Writer{store: s, version: version, base: base}
	if err := w.init(); err != nil {
		s.releaseCommitSlot()
		return nil, err
	}
	return w, nil
}

// Latest 返回最新已提交版本号；无已提交检查点时返回 ErrNoCheckpoint。
func (s *Store) Latest() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	versions, err := s.listLocked()
	if err != nil {
		return 0, err
	}
	if len(versions) == 0 {
		return 0, ErrNoCheckpoint
	}
	return versions[len(versions)-1], nil
}

// listLocked 返回所有已提交版本（升序）。调用方需持有 s.mu。
func (s *Store) listLocked() ([]uint64, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, committedDir))
	if err != nil {
		return nil, fmt.Errorf("checkpoint: list committed: %w", err)
	}
	var versions []uint64
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		v, ok := parseVersionDir(e.Name())
		if !ok {
			continue
		}
		// 只有包含 manifest 的目录才算已提交；rename 原子性保证不会出现
		// “目录存在但 manifest 缺失”的已提交目录，防御性跳过即可。
		if _, err := os.Stat(filepath.Join(s.committedPath(v), manifestFile)); err != nil {
			log.Printf("checkpoint: skip incomplete dir version=%d: %v", v, err)
			continue
		}
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	return versions, nil
}

// List 返回所有已提交版本（升序）。
func (s *Store) List() ([]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listLocked()
}

// Recover 恢复指定版本（version==0 表示最新已提交版本），
// 校验分片完整性，返回清单与完整性报告。
// 完整性失败时返回 ErrShardIntegrity 与携带失败原因的报告。
func (s *Store) Recover(version uint64) (*Manifest, *IntegrityReport, error) {
	if version == 0 {
		latest, err := s.Latest()
		if err != nil {
			return nil, nil, err
		}
		version = latest
	}
	dir := s.committedPath(version)
	data, err := os.ReadFile(filepath.Join(dir, manifestFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, fmt.Errorf("%w: version=%d", ErrIncompleteCommit, version)
		}
		return nil, nil, fmt.Errorf("checkpoint: read manifest: %w", err)
	}
	m, err := decodeManifest(data)
	if err != nil {
		return nil, nil, err
	}
	log.Printf("checkpoint: recover version=%d format=%d shards=%d bytes=%d",
		m.Version, m.Format, len(m.Shards), m.TotalBytes())

	report := s.verifyShards(dir, m)
	if !report.Complete {
		for _, f := range report.Failures {
			log.Printf("checkpoint: shard failure version=%d index=%d reason=%s detail=%s",
				m.Version, f.Index, f.Reason, f.Detail)
		}
		return m, report, fmt.Errorf("%w: version=%d failures=%d", ErrShardIntegrity, m.Version, len(report.Failures))
	}
	return m, report, nil
}

// verifyShards 对照清单校验磁盘上的分片，生成完整性报告。
func (s *Store) verifyShards(dir string, m *Manifest) *IntegrityReport {
	report := &IntegrityReport{Version: m.Version, Complete: true}
	fail := func(idx int, reason ShardFailureReason, format string, args ...any) {
		report.Complete = false
		report.Failures = append(report.Failures, ShardFailure{
			Index: idx, Reason: reason, Detail: fmt.Sprintf(format, args...),
		})
	}

	shardDir := filepath.Join(dir, shardsDir)
	seen := make(map[string]bool, len(m.Shards))
	for _, sm := range m.Shards {
		name := fmt.Sprintf(shardNameFmt, sm.Index)
		seen[name] = true
		path := filepath.Join(shardDir, name)
		fi, err := os.Stat(path)
		if err != nil {
			fail(sm.Index, ShardMissing, "stat: %v", err)
			continue
		}
		if fi.Size() != sm.Size {
			fail(sm.Index, ShardTruncated, "size=%d want=%d", fi.Size(), sm.Size)
			continue
		}
		sum, err := hashFile(path)
		if err != nil {
			fail(sm.Index, ShardMissing, "read: %v", err)
			continue
		}
		if sum != sm.SHA256 {
			fail(sm.Index, ShardCorrupted, "sha256=%s want=%s", sum, sm.SHA256)
		}
	}

	// 清单未声明的孤儿分片：可能是错位写入或旧提交残留。
	if entries, err := os.ReadDir(shardDir); err == nil {
		for _, e := range entries {
			if !seen[e.Name()] {
				report.Complete = false
				report.Failures = append(report.Failures, ShardFailure{
					Index: -1, Reason: ShardOrphan, Detail: "unexpected file " + e.Name(),
				})
			}
		}
	}
	return report
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, io.LimitReader(f, maxShardReadUp)); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ReadShard 读取已提交检查点中指定分片的内容，并校验其完整性。
func (s *Store) ReadShard(version uint64, index int) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(s.committedPath(version), manifestFile))
	if err != nil {
		return nil, fmt.Errorf("%w: version=%d", ErrIncompleteCommit, version)
	}
	m, err := decodeManifest(data)
	if err != nil {
		return nil, err
	}
	if index < 0 || index >= len(m.Shards) {
		return nil, fmt.Errorf("checkpoint: shard index %d out of range [0,%d)", index, len(m.Shards))
	}
	sm := m.Shards[index]
	raw, err := os.ReadFile(filepath.Join(s.committedPath(version), shardsDir, fmt.Sprintf(shardNameFmt, index)))
	if err != nil {
		return nil, fmt.Errorf("%w: shard %d: %v", ErrShardIntegrity, index, err)
	}
	if int64(len(raw)) != sm.Size {
		return nil, fmt.Errorf("%w: shard %d truncated size=%d want=%d", ErrShardIntegrity, index, len(raw), sm.Size)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != sm.SHA256 {
		return nil, fmt.Errorf("%w: shard %d corrupted", ErrShardIntegrity, index)
	}
	return raw, nil
}

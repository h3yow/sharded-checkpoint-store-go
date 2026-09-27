package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	chkDirPrefix  = "chk-"
	manifestName  = "manifest.json"
	manifestTmp   = "manifest.tmp"
	shardFileTmpl = "shard-%05d.ckpt"
)

// Store 是本地分片检查点存储，并发安全。
type Store struct {
	dir string
	cfg Config

	mu        sync.Mutex
	committed uint64 // 当前已提交的最高版本，0 表示无
	pending   map[uint64]struct{}
	inflight  chan struct{}
	budget    *stagingBudget // 暂存容量预算，nil 表示不限制
}

// Open 打开（必要时创建）位于 dir 的存储，并扫描已提交的最高版本。
func Open(dir string, cfg *Config) (*Store, error) {
	c := Config{}
	if cfg != nil {
		c = *cfg
	}
	s := &Store{
		dir:      dir,
		cfg:      c.withDefaults(),
		pending:  map[uint64]struct{}{},
		inflight: make(chan struct{}, c.withDefaults().MaxInflight),
	}
	if s.cfg.MaxStagedBytes > 0 {
		s.budget = newStagingBudget(s.cfg.MaxStagedBytes, s.cfg.Staging)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("checkpoint: create store dir: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: scan store dir: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), chkDirPrefix) {
			continue
		}
		v, err := strconv.ParseUint(strings.TrimPrefix(e.Name(), chkDirPrefix), 10, 64)
		if err != nil {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, e.Name(), manifestName)); err == nil {
			if v > s.committed {
				s.committed = v
			}
		} else {
			log.Printf("checkpoint: open: dir %s has no manifest, treated as incomplete commit", e.Name())
		}
	}
	return s, nil
}

// Latest 返回当前已提交的最高版本号，无提交时返回 ErrNoCheckpoint。
func (s *Store) Latest() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.committed == 0 {
		return 0, ErrNoCheckpoint
	}
	return s.committed, nil
}

func (s *Store) chkDir(version uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%s%d", chkDirPrefix, version))
}

// Writer 是一次进行中的写入事务。
// 分片先暂存在内存（计入存储级暂存配额），Commit 时统一落盘。
type Writer struct {
	store   *Store
	version uint64
	dir     string

	mu          sync.Mutex
	staged      map[int][]byte // 暂存的分片 payload
	stagedBytes int64          // 本事务占用的暂存配额
	finished    bool
}

// admit 校验版本准入并登记挂起事务、占用并发槽。
// 任何无法通过正常恢复入口读回或违反版本顺序的编号都在这里被拒绝，
// 成功后调用方负责通过 release 释放。
func (s *Store) admit(ctx context.Context, version uint64) error {
	// version 0 被 Recover(0)=最新版本 的语义占用，提交后将无法按编号寻址，
	// 必须在开始写入前拒绝。
	if version == 0 {
		return fmt.Errorf("%w: version 0 is reserved for latest and cannot be committed", ErrInvalidVersion)
	}
	s.mu.Lock()
	if version < s.committed {
		s.mu.Unlock()
		return fmt.Errorf("%w: version %d < committed %d", ErrVersionRegression, version, s.committed)
	}
	if version == s.committed {
		s.mu.Unlock()
		return fmt.Errorf("%w: version %d", ErrVersionExists, version)
	}
	if _, dup := s.pending[version]; dup {
		s.mu.Unlock()
		return fmt.Errorf("%w: version %d already has a pending writer", ErrResourceLimit, version)
	}
	s.pending[version] = struct{}{}
	s.mu.Unlock()

	select {
	case s.inflight <- struct{}{}:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.pending, version)
		s.mu.Unlock()
		return ctx.Err()
	}
}

// Begin 开启对 version 的写入事务。
// 重复提交已提交版本返回 ErrVersionExists；版本号回退返回 ErrVersionRegression；
// version 为 0 返回 ErrInvalidVersion；并发事务数超限时阻塞直至有空位或 ctx 取消。
func (s *Store) Begin(ctx context.Context, version uint64) (*Writer, error) {
	if err := s.admit(ctx, version); err != nil {
		return nil, err
	}
	dir := s.chkDir(version)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		s.release(version)
		return nil, fmt.Errorf("checkpoint: create chk dir: %w", err)
	}
	log.Printf("checkpoint: begin version=%d dir=%s", version, dir)
	return &Writer{store: s, version: version, dir: dir, staged: map[int][]byte{}}, nil
}

// BeginFrom 开启派生写入事务：新版本 version 承接已提交版本 baseVersion 的
// 全部分片内容，之后可通过 WriteShard 覆盖或追加部分分片，Commit 后恢复结果
// 与“承接内容 + 本次改动”的全量重写一致；来源版本不受影响。
// baseVersion 为 0 时承接最新已提交版本。
// 来源版本不存在、未完整提交或任何分片读不出来时，在落任何新数据之前失败；
// 来源为较老格式版本时按既定兼容规则确定性读取。
func (s *Store) BeginFrom(ctx context.Context, version, baseVersion uint64) (*Writer, error) {
	if err := s.admit(ctx, version); err != nil {
		return nil, err
	}
	// 先完整恢复来源版本：此时新数据尚未落盘，失败即干净退出，无残留。
	base, _, err := s.Recover(baseVersion)
	if err != nil {
		s.release(version)
		return nil, fmt.Errorf("checkpoint: derive version %d from base %d: %w", version, baseVersion, err)
	}
	w := &Writer{store: s, version: version, dir: s.chkDir(version), staged: map[int][]byte{}}
	for i := 0; i < len(base); i++ {
		payload, ok := base[i]
		if !ok {
			w.releaseBudget()
			s.release(version)
			return nil, fmt.Errorf("checkpoint: derive version %d: base %d shard %d missing",
				version, baseVersion, i)
		}
		if err := w.stage(ctx, i, payload); err != nil {
			w.releaseBudget()
			s.release(version)
			return nil, fmt.Errorf("checkpoint: derive version %d: stage base shard %d: %w",
				version, i, err)
		}
	}
	if err := os.MkdirAll(w.dir, 0o755); err != nil {
		w.releaseBudget()
		s.release(version)
		return nil, fmt.Errorf("checkpoint: create chk dir: %w", err)
	}
	log.Printf("checkpoint: begin version=%d derived from base=%d shards=%d",
		version, baseVersion, len(base))
	return w, nil
}

// stage 申请暂存配额并暂存一个分片 payload；覆盖同序号分片时按差额增减配额。
// 调用方需持有 w.mu（或事务尚未共享）。
func (w *Writer) stage(ctx context.Context, index int, payload []byte) error {
	// 复制一份，避免调用方后续修改底层切片影响暂存内容。
	payload = append([]byte(nil), payload...)
	delta := int64(len(payload)) - int64(len(w.staged[index]))
	b := w.store.budget
	if b != nil && delta > 0 {
		if err := b.acquire(ctx, delta); err != nil {
			return err
		}
	}
	if b != nil && delta < 0 {
		b.release(-delta)
	}
	w.staged[index] = payload
	w.stagedBytes += delta
	return nil
}

// releaseBudget 归还本事务占用的全部暂存配额。
func (w *Writer) releaseBudget() {
	if b := w.store.budget; b != nil && w.stagedBytes > 0 {
		b.release(w.stagedBytes)
	}
	w.stagedBytes = 0
}

// teardown 释放事务占用的暂存配额与版本槽位，每个结束的事务恰好调用一次。
func (w *Writer) teardown() {
	w.releaseBudget()
	w.store.release(w.version)
}

// WriteShard 暂存一个分片；超过配置上限返回 ErrResourceLimit。
// 暂存策略为 StagingWait 时，容量不足会阻塞等待，直到有空位或 ctx 取消。
func (w *Writer) WriteShard(ctx context.Context, index int, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finished {
		return fmt.Errorf("checkpoint: writer for version %d already finished", w.version)
	}
	if index < 0 || index >= w.store.cfg.MaxShards {
		return fmt.Errorf("%w: shard index %d out of range [0, %d)",
			ErrResourceLimit, index, w.store.cfg.MaxShards)
	}
	if int64(len(payload)) > w.store.cfg.MaxShardBytes {
		return fmt.Errorf("%w: shard %d payload %d bytes > limit %d",
			ErrResourceLimit, index, len(payload), w.store.cfg.MaxShardBytes)
	}
	if err := w.stage(ctx, index, payload); err != nil {
		return err
	}
	log.Printf("checkpoint: version=%d shard=%d bytes=%d staged", w.version, index, len(payload))
	return nil
}

// Commit 原子提交：先把暂存的全部分片落盘并 fsync，再写临时清单，
// 最后在持锁校验版本准入后原子重命名清单完成提交点。
func (w *Writer) Commit() error {
	w.mu.Lock()
	if w.finished {
		w.mu.Unlock()
		return fmt.Errorf("checkpoint: writer for version %d already finished", w.version)
	}
	w.finished = true
	if len(w.staged) == 0 {
		w.mu.Unlock()
		w.teardown()
		return fmt.Errorf("checkpoint: commit version %d with no shards", w.version)
	}
	m := &Manifest{
		FormatVersion: MaxFormatVersion,
		Version:       w.version,
		ShardCount:    len(w.staged),
		Shards:        make([]ShardMeta, 0, len(w.staged)),
	}
	// 暂存内容统一落盘：逐个编码、写入并 fsync 分片文件。
	for i := 0; i < len(w.staged); i++ {
		payload, ok := w.staged[i]
		if !ok {
			w.mu.Unlock()
			w.teardown()
			return fmt.Errorf("checkpoint: commit version %d: shard %d missing, indices must be contiguous",
				w.version, i)
		}
		data := encodeShard(MaxFormatVersion, i, payload)
		path := filepath.Join(w.dir, fmt.Sprintf(shardFileTmpl, i))
		if err := writeFileSync(path, data); err != nil {
			w.mu.Unlock()
			w.teardown()
			return fmt.Errorf("checkpoint: write shard %d: %w", i, err)
		}
		m.Shards = append(m.Shards, ShardMeta{
			Index:  i,
			Size:   int64(len(data)),
			CRC32:  shardCRC(data),
			Chunks: 1,
		})
	}
	w.mu.Unlock()

	data, err := encodeManifest(m)
	if err != nil {
		w.teardown()
		return err
	}
	tmp := filepath.Join(w.dir, manifestTmp)
	if err := writeFileSync(tmp, data); err != nil {
		w.teardown()
		return fmt.Errorf("checkpoint: write manifest: %w", err)
	}
	// 提交点：持锁重新校验版本准入——若本事务准备期间已有更高版本提交，
	// 本版本不得再落盘成功，清理临时目录后拒绝。
	s := w.store
	s.mu.Lock()
	if w.version <= s.committed {
		committed := s.committed
		s.mu.Unlock()
		os.RemoveAll(w.dir)
		w.teardown()
		return fmt.Errorf("%w: version %d <= committed %d at commit time",
			ErrVersionRegression, w.version, committed)
	}
	// 原子重命名即提交点：此前崩溃只会留下无清单的目录，恢复时判为未完整提交。
	if err := os.Rename(tmp, filepath.Join(w.dir, manifestName)); err != nil {
		s.mu.Unlock()
		w.teardown()
		return fmt.Errorf("checkpoint: commit rename: %w", err)
	}
	syncDir(w.dir)
	s.committed = w.version
	s.mu.Unlock()
	w.teardown()
	log.Printf("checkpoint: commit version=%d shards=%d", w.version, m.ShardCount)
	return nil
}

// Abort 中止事务并清理临时数据，不得残留可被误认为有效的内容。
func (w *Writer) Abort() error {
	w.mu.Lock()
	if w.finished {
		w.mu.Unlock()
		return fmt.Errorf("checkpoint: writer for version %d already finished", w.version)
	}
	w.finished = true
	w.mu.Unlock()
	// 未提交目录中没有 manifest.json，本就不会被当作有效检查点；
	// 这里仍整体删除，避免残留垃圾。
	if err := os.RemoveAll(w.dir); err != nil {
		w.teardown()
		return fmt.Errorf("checkpoint: abort cleanup: %w", err)
	}
	w.teardown()
	log.Printf("checkpoint: abort version=%d", w.version)
	return nil
}

func (s *Store) release(version uint64) {
	s.mu.Lock()
	delete(s.pending, version)
	s.mu.Unlock()
	<-s.inflight
}

// Recover 恢复指定版本（version 为 0 时取最新已提交版本），
// 返回数据与分片完整性报告；未完整提交的检查点返回 ErrIncompleteCommit。
func (s *Store) Recover(version uint64) (map[int][]byte, *RecoveryReport, error) {
	if version == 0 {
		v, err := s.Latest()
		if err != nil {
			return nil, nil, err
		}
		version = v
	}
	dir := s.chkDir(version)
	mdata, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if _, derr := os.Stat(dir); derr == nil {
				return nil, nil, fmt.Errorf("%w: version %d has no manifest",
					ErrIncompleteCommit, version)
			}
			return nil, nil, fmt.Errorf("%w: version %d not found", ErrNoCheckpoint, version)
		}
		return nil, nil, fmt.Errorf("checkpoint: read manifest: %w", err)
	}
	m, err := decodeManifest(mdata)
	if err != nil {
		return nil, nil, err
	}
	rep := &RecoveryReport{Version: m.Version, Format: m.FormatVersion}
	out := map[int][]byte{}
	var firstErr error
	for _, sh := range m.Shards {
		sr := ShardReport{Index: sh.Index}
		payload, err := s.recoverShard(dir, m.FormatVersion, sh)
		if err != nil {
			sr.Err = err
			if firstErr == nil {
				firstErr = err
			}
			log.Printf("checkpoint: recover version=%d shard=%d FAILED: %v", m.Version, sh.Index, err)
		} else {
			sr.OK = true
			out[sh.Index] = payload
		}
		rep.Shards = append(rep.Shards, sr)
	}
	if firstErr != nil {
		return nil, rep, firstErr
	}
	log.Printf("checkpoint: recover version=%d shards=%d ok", m.Version, len(rep.Shards))
	return out, rep, nil
}

func (s *Store) recoverShard(dir string, format int, sh ShardMeta) ([]byte, error) {
	path := filepath.Join(dir, fmt.Sprintf(shardFileTmpl, sh.Index))
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: shard %d", ErrShardMissing, sh.Index)
		}
		return nil, fmt.Errorf("checkpoint: open shard %d: %w", sh.Index, err)
	}
	defer f.Close()
	meta, payload, err := decodeShard(f)
	if err != nil {
		return nil, err
	}
	if meta.Size != sh.Size {
		return nil, fmt.Errorf("%w: shard %d size %d != manifest %d",
			ErrShardCorrupted, sh.Index, meta.Size, sh.Size)
	}
	// 格式 v2 起清单记录 CRC32，需与分片实际校验和一致；
	// v1 清单 CRC32 缺省为 0，确定性地退化为仅依赖分片文件内部校验。
	if format >= 2 && sh.CRC32 != 0 && meta.CRC32 != sh.CRC32 {
		return nil, fmt.Errorf("%w: shard %d crc %08x != manifest %08x",
			ErrShardCorrupted, sh.Index, meta.CRC32, sh.CRC32)
	}
	return payload, nil
}

func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}

// shardCRC 从已编码的分片字节中提取尾部 CRC32。
func shardCRC(data []byte) uint32 {
	if len(data) < shardTrailerLen {
		return 0
	}
	return uint32(data[len(data)-4]) | uint32(data[len(data)-3])<<8 |
		uint32(data[len(data)-2])<<16 | uint32(data[len(data)-1])<<24
}

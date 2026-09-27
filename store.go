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
	staging   *stagingLimiter
	commitMu  sync.Mutex // 序列化提交临界区，保证版本准入复核与落盘整体串行
}

// Open 打开（必要时创建）位于 dir 的存储，并扫描已提交的最高版本。
func Open(dir string, cfg *Config) (*Store, error) {
	c := Config{}
	if cfg != nil {
		c = *cfg
	}
	d := c.withDefaults()
	s := &Store{
		dir:      dir,
		cfg:      d,
		pending:  map[uint64]struct{}{},
		inflight: make(chan struct{}, d.MaxInflight),
		staging:  newStagingLimiter(d.MaxStagedBytes, d.StagingPolicy),
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
// 分片数据先暂存在内存（受 Store 暂存总量限额约束），Commit 时统一落盘。
type Writer struct {
	store   *Store
	version uint64
	dir     string

	mu       sync.Mutex
	staged   map[int][]byte // 暂存的分片 payload（含派生承接的基础分片）
	held     int64          // 已从暂存限额申请的字节总量
	finished bool
}

// admit 做版本准入校验并登记 pending、占用 inflight 槽位。
// 在真正开始写入（落盘）前拒绝一切最终无法经正常恢复入口读回的版本：
//   - version == 0：Recover(0) 语义为“最新版本”，v0 提交后永远无法读回；
//   - version <= 已提交版本：重复/覆盖/回退；
//   - 同一版本已有进行中的写入事务。
func (s *Store) admit(ctx context.Context, version uint64) error {
	if version == 0 {
		return fmt.Errorf("%w: version 0 can never be recovered (0 means latest)", ErrInvalidVersion)
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

// WriteShard 暂存一个分片；超过配置上限返回 ErrResourceLimit。
// 数据先暂存在内存并计入 Store 级暂存总量限额：拒绝策略下超限立即失败；
// 等待策略下阻塞至有空位或 ctx 取消。Commit 时才统一落盘。
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
	// 覆盖同序号分片时按差额记账，避免重复占用额度。
	delta := int64(len(payload)) - int64(len(w.staged[index]))
	if delta > 0 {
		if err := w.store.staging.acquire(ctx, delta); err != nil {
			return err
		}
	} else if delta < 0 {
		w.store.staging.release(-delta)
	}
	// 拷贝一份，调用方后续修改入参不影响已暂存内容。
	cp := make([]byte, len(payload))
	copy(cp, payload)
	w.staged[index] = cp
	w.held += delta
	log.Printf("checkpoint: version=%d shard=%d bytes=%d staged", w.version, index, len(payload))
	return nil
}

// Commit 原子提交：先在提交临界区内复核版本准入（防止低版本在高版本
// 已提交后悄悄落盘），再落盘全部分片，最后原子重命名清单完成提交点。
func (w *Writer) Commit() error {
	w.mu.Lock()
	if w.finished {
		w.mu.Unlock()
		return fmt.Errorf("checkpoint: writer for version %d already finished", w.version)
	}
	w.finished = true
	staged := w.staged
	held := w.held
	w.mu.Unlock()

	s := w.store
	// 失败路径统一释放暂存额度与事务槽位。
	fail := func(err error) error {
		s.staging.release(held)
		s.release(w.version)
		return err
	}

	if len(staged) == 0 {
		return fail(fmt.Errorf("checkpoint: commit version %d with no shards", w.version))
	}
	for i := 0; i < len(staged); i++ {
		if _, ok := staged[i]; !ok {
			return fail(fmt.Errorf("checkpoint: commit version %d: shard %d missing, indices must be contiguous",
				w.version, i))
		}
	}

	// 提交临界区：版本准入复核、落盘、更新 committed 必须整体串行，
	// 否则可能出现“低版本准备中、高版本已提交，低版本仍落盘成功”的错位。
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	s.mu.Lock()
	committed := s.committed
	s.mu.Unlock()
	if w.version <= committed {
		err := fmt.Errorf("%w: version %d", ErrVersionExists, w.version)
		if w.version < committed {
			err = fmt.Errorf("%w: version %d < committed %d", ErrVersionRegression, w.version, committed)
		}
		// 尚未落任何数据，目录为空；整体删除，不留任何疑似有效版本的残留。
		if rmErr := os.RemoveAll(w.dir); rmErr != nil {
			log.Printf("checkpoint: stale commit cleanup version=%d: %v", w.version, rmErr)
		}
		return fail(err)
	}

	m := &Manifest{
		FormatVersion: MaxFormatVersion,
		Version:       w.version,
		ShardCount:    len(staged),
		Shards:        make([]ShardMeta, 0, len(staged)),
	}
	for i := 0; i < len(staged); i++ {
		data := encodeShard(MaxFormatVersion, i, staged[i])
		path := filepath.Join(w.dir, fmt.Sprintf(shardFileTmpl, i))
		if err := writeFileSync(path, data); err != nil {
			return fail(fmt.Errorf("checkpoint: write shard %d: %w", i, err))
		}
		m.Shards = append(m.Shards, ShardMeta{
			Index:  i,
			Size:   int64(len(data)),
			CRC32:  shardCRC(data),
			Chunks: 1,
		})
	}
	data, err := encodeManifest(m)
	if err != nil {
		return fail(err)
	}
	tmp := filepath.Join(w.dir, manifestTmp)
	if err := writeFileSync(tmp, data); err != nil {
		return fail(fmt.Errorf("checkpoint: write manifest: %w", err))
	}
	// 原子重命名即提交点：此前崩溃只会留下无清单的目录，恢复时判为未完整提交。
	if err := os.Rename(tmp, filepath.Join(w.dir, manifestName)); err != nil {
		return fail(fmt.Errorf("checkpoint: commit rename: %w", err))
	}
	syncDir(w.dir)
	s.mu.Lock()
	if w.version > s.committed {
		s.committed = w.version
	}
	s.mu.Unlock()
	s.staging.release(held)
	s.release(w.version)
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
	held := w.held
	w.mu.Unlock()
	// 未提交目录中没有 manifest.json，本就不会被当作有效检查点；
	// 这里仍整体删除，避免残留垃圾。
	if err := os.RemoveAll(w.dir); err != nil {
		w.store.staging.release(held)
		w.store.release(w.version)
		return fmt.Errorf("checkpoint: abort cleanup: %w", err)
	}
	w.store.staging.release(held)
	w.store.release(w.version)
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

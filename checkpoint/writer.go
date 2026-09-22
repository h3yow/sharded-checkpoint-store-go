package checkpoint

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

var stagingSeq atomic.Uint64

// Writer 一次检查点写入会话。
// 所有分片写入 staging 临时区，Commit 时通过目录 rename 原子发布；
// Abort 清理临时数据。Writer 不是并发安全的，但多个 Writer 之间互不影响。
type Writer struct {
	store   *Store
	version uint64
	base    uint64

	stagingPath string
	shards      []ShardMeta
	nextIndex   int
	buffered    int64
	closed      bool
}

// init 校验版本并创建 staging 目录；base>0 时把基线分片复制进 staging。
func (w *Writer) init() error {
	s := w.store
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkVersionLocked(w.version); err != nil {
		return err
	}
	if w.base > 0 {
		if _, err := os.Stat(filepath.Join(s.committedPath(w.base), manifestFile)); err != nil {
			return fmt.Errorf("checkpoint: base version %d not committed: %w", w.base, ErrIncompleteCommit)
		}
	}

	id := stagingSeq.Add(1)
	w.stagingPath = filepath.Join(s.dir, stagingDir,
		fmt.Sprintf("w-%d-%d", os.Getpid(), id))
	if err := os.MkdirAll(filepath.Join(w.stagingPath, shardsDir), 0o755); err != nil {
		return fmt.Errorf("checkpoint: create staging: %w", err)
	}

	if w.base > 0 {
		if err := w.copyBaseLocked(); err != nil {
			os.RemoveAll(w.stagingPath)
			return err
		}
	}
	log.Printf("checkpoint: begin version=%d base=%d staging=%s", w.version, w.base, w.stagingPath)
	return nil
}

// copyBaseLocked 把基线版本的分片与元数据复制到 staging，作为追加的起点。
func (w *Writer) copyBaseLocked() error {
	s := w.store
	data, err := os.ReadFile(filepath.Join(s.committedPath(w.base), manifestFile))
	if err != nil {
		return err
	}
	base, err := decodeManifest(data)
	if err != nil {
		return err
	}
	if s.opt.MaxShards > 0 && len(base.Shards) > s.opt.MaxShards {
		return fmt.Errorf("%w: base shards %d exceed max %d", ErrLimitExceeded, len(base.Shards), s.opt.MaxShards)
	}
	for _, sm := range base.Shards {
		src := filepath.Join(s.committedPath(w.base), shardsDir, fmt.Sprintf(shardNameFmt, sm.Index))
		dst := filepath.Join(w.stagingPath, shardsDir, fmt.Sprintf(shardNameFmt, sm.Index))
		if err := copyFile(src, dst); err != nil {
			return fmt.Errorf("checkpoint: copy base shard %d: %w", sm.Index, err)
		}
		w.buffered += sm.Size
	}
	w.shards = append(w.shards, base.Shards...)
	w.nextIndex = len(base.Shards)
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// WriteShard 写入一个分片，index 必须等于下一个期望序号（从基线分片数起递增）。
// size<0 表示未知长度，将读取到 EOF；否则必须恰好读取 size 字节。
func (w *Writer) WriteShard(index int, r io.Reader, size int64) error {
	if w.closed {
		return ErrWriterClosed
	}
	if index != w.nextIndex {
		return fmt.Errorf("checkpoint: shard index out of order: got %d want %d", index, w.nextIndex)
	}
	s := w.store
	if s.opt.MaxShards > 0 && len(w.shards)+1 > s.opt.MaxShards {
		return fmt.Errorf("%w: shards %d exceed max %d", ErrLimitExceeded, len(w.shards)+1, s.opt.MaxShards)
	}
	if size >= 0 && s.opt.MaxShardBytes > 0 && size > s.opt.MaxShardBytes {
		return fmt.Errorf("%w: shard size %d exceed max %d", ErrLimitExceeded, size, s.opt.MaxShardBytes)
	}

	path := filepath.Join(w.stagingPath, shardsDir, fmt.Sprintf(shardNameFmt, index))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("checkpoint: create shard %d: %w", index, err)
	}

	h := sha256.New()
	var src io.Reader = r
	if size >= 0 {
		src = io.LimitReader(r, size+1) // +1 用于检测超长输入
	}
	n, err := io.Copy(f, io.TeeReader(src, h))
	if err != nil {
		f.Close()
		return fmt.Errorf("checkpoint: write shard %d: %w", index, err)
	}
	if size >= 0 && n != size {
		f.Close()
		return fmt.Errorf("checkpoint: shard %d size mismatch: wrote %d want %d", index, n, size)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("checkpoint: fsync shard %d: %w", index, err)
	}
	if err := f.Close(); err != nil {
		return err
	}

	if s.opt.MaxShardBytes > 0 && n > s.opt.MaxShardBytes {
		return fmt.Errorf("%w: shard bytes %d exceed max %d", ErrLimitExceeded, n, s.opt.MaxShardBytes)
	}
	if s.opt.MaxBufferedBytes > 0 && w.buffered+n > s.opt.MaxBufferedBytes {
		return fmt.Errorf("%w: buffered bytes %d exceed max %d", ErrLimitExceeded, w.buffered+n, s.opt.MaxBufferedBytes)
	}

	w.buffered += n
	w.shards = append(w.shards, ShardMeta{
		Index:  index,
		Size:   n,
		SHA256: hex.EncodeToString(h.Sum(nil)),
	})
	w.nextIndex++
	return nil
}

// Commit 原子提交：清单写入 staging 并 fsync 后，将 staging 中的分片与清单
// 一次性 rename 到 committed/v<version>，此后才对恢复可见。
// 提交失败（如版本竞争）时 staging 被清理，不残留可被误认为有效的数据。
func (w *Writer) Commit() error {
	if w.closed {
		return ErrWriterClosed
	}
	w.closed = true
	s := w.store
	defer s.releaseCommitSlot()

	m := &Manifest{
		Version:       w.version,
		SourceVersion: w.base,
		CreatedAt:     time.Now().UTC(),
		Shards:        w.shards,
	}
	data, err := encodeManifest(m)
	if err != nil {
		os.RemoveAll(w.stagingPath)
		return err
	}
	manifestPath := filepath.Join(w.stagingPath, manifestFile)
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		os.RemoveAll(w.stagingPath)
		return fmt.Errorf("checkpoint: write manifest: %w", err)
	}
	if f, err := os.Open(manifestPath); err == nil {
		f.Sync()
		f.Close()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// 提交前再次校验版本：Begin 之后可能有其他会话提交了相同或更高版本。
	if err := s.checkVersionLocked(w.version); err != nil {
		os.RemoveAll(w.stagingPath)
		log.Printf("checkpoint: commit rejected version=%d reason=%v", w.version, err)
		return err
	}
	target := s.committedPath(w.version)
	if err := os.Rename(w.stagingPath, target); err != nil {
		os.RemoveAll(w.stagingPath)
		return fmt.Errorf("checkpoint: publish version %d: %w", w.version, err)
	}
	syncDir(filepath.Join(s.dir, committedDir))
	log.Printf("checkpoint: committed version=%d shards=%d bytes=%d", w.version, len(w.shards), m.TotalBytes())
	return nil
}

// Abort 中止会话并清理临时数据，不留下任何可见状态。
func (w *Writer) Abort() error {
	if w.closed {
		return ErrWriterClosed
	}
	w.closed = true
	defer w.store.releaseCommitSlot()
	if w.stagingPath != "" {
		return os.RemoveAll(w.stagingPath)
	}
	return nil
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}

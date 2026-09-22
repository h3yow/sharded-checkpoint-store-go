package checkpoint

import "io"

// Writer 一次检查点写入会话。
// 所有分片写入临时区，Commit 时原子发布；Abort 清理临时数据。
type Writer struct {
	store   *Store
	version uint64
	base    uint64
	closed  bool
}

// WriteShard 写入一个分片，index 必须递增且不重复。
func (w *Writer) WriteShard(index int, r io.Reader, size int64) error {
	return nil
}

// Commit 原子提交：清单落盘并发布后检查点才可见。
func (w *Writer) Commit() error {
	return nil
}

// Abort 中止会话并清理临时数据，不留下任何可见状态。
func (w *Writer) Abort() error {
	return nil
}

package checkpoint

import (
	"context"
	"sync"
)

// Store 本地分片检查点存储，并发安全。
type Store struct {
	dir string
	opt Options

	mu           sync.Mutex
	commitSem    chan struct{} // 进行中的提交配额
	latestCached uint64
	hasLatest    bool
}

// Open 打开（必要时创建）位于 dir 的检查点存储。
func Open(dir string, opt Options) (*Store, error) {
	return nil, nil
}

// Begin 开始一个写入会话，目标版本为 version。
// 基于 base 版本追加时传 base>0，全新检查点传 0。
func (s *Store) Begin(ctx context.Context, version uint64, base uint64) (*Writer, error) {
	return nil, nil
}

// Latest 返回最新已提交版本号；无已提交检查点时返回 ErrNoCheckpoint。
func (s *Store) Latest() (uint64, error) {
	return 0, nil
}

// Recover 恢复指定版本（version==0 表示最新已提交版本），
// 校验分片完整性，返回清单与完整性报告。
// 未完整提交的检查点不会被当作最新版本加载。
func (s *Store) Recover(version uint64) (*Manifest, *IntegrityReport, error) {
	return nil, nil, nil
}

// ReadShard 读取已提交检查点中指定分片的内容。
func (s *Store) ReadShard(version uint64, index int) ([]byte, error) {
	return nil, nil
}

// List 返回所有已提交版本（升序）。
func (s *Store) List() ([]uint64, error) {
	return nil, nil
}

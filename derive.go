package checkpoint

import (
	"context"
	"fmt"
	"log"
	"os"
)

// BeginDerive 开启一个“基于已有版本继续演进”的写入事务：
// 新版本 version 承接来源版本 sourceVersion 的全部分片内容，
// 之后只需用 WriteShard 覆盖/追加需要改动的分片，Commit 后
// 恢复新版本看到的就是“承接内容 + 本次改动”的完整结果，
// 与把旧版本全量重写一遍再改动等价；来源版本不受任何影响。
//
// 来源版本不存在、未完整提交或读不出来时，在落任何新数据之前失败
// （不会创建新版本目录，不留任何疑似有效版本的残留）。
// 来源为较老格式版本（如 v1）时，读取结论与 Recover 一样确定：
// 缺失字段按既定缺省规则处理，不会时好时坏。
//
// 承接的基础分片同样计入 Store 级暂存总量限额。
func (s *Store) BeginDerive(ctx context.Context, version, sourceVersion uint64) (*Writer, error) {
	// 先做版本准入（占用 pending/inflight），再读来源，最后才建目录：
	// 任何一步失败都不会留下新版本的落盘痕迹。
	if err := s.admit(ctx, version); err != nil {
		return nil, err
	}
	base, rep, err := s.Recover(sourceVersion)
	if err != nil {
		s.release(version)
		return nil, fmt.Errorf("checkpoint: derive version %d from %d: source unavailable: %w",
			version, sourceVersion, err)
	}
	if !rep.OK() {
		s.release(version)
		return nil, fmt.Errorf("checkpoint: derive version %d from %d: source report not OK", version, sourceVersion)
	}
	if len(base) > s.cfg.MaxShards {
		s.release(version)
		return nil, fmt.Errorf("%w: source version %d has %d shards > limit %d",
			ErrResourceLimit, sourceVersion, len(base), s.cfg.MaxShards)
	}

	// 承接内容计入暂存限额：拒绝策略下超限立即失败；等待策略下阻塞或随 ctx 取消。
	staged := make(map[int][]byte, len(base))
	var held int64
	for i := 0; i < len(base); i++ {
		payload, ok := base[i]
		if !ok {
			s.staging.release(held)
			s.release(version)
			return nil, fmt.Errorf("checkpoint: derive version %d from %d: source shard %d missing",
				version, sourceVersion, i)
		}
		if int64(len(payload)) > s.cfg.MaxShardBytes {
			s.staging.release(held)
			s.release(version)
			return nil, fmt.Errorf("%w: source shard %d payload %d bytes > limit %d",
				ErrResourceLimit, i, len(payload), s.cfg.MaxShardBytes)
		}
		if err := s.staging.acquire(ctx, int64(len(payload))); err != nil {
			s.staging.release(held)
			s.release(version)
			return nil, fmt.Errorf("checkpoint: derive version %d from %d: staging: %w",
				version, sourceVersion, err)
		}
		held += int64(len(payload))
		staged[i] = payload
	}

	dir := s.chkDir(version)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		s.staging.release(held)
		s.release(version)
		return nil, fmt.Errorf("checkpoint: create chk dir: %w", err)
	}
	log.Printf("checkpoint: begin derive version=%d from=%d shards=%d", version, sourceVersion, len(staged))
	return &Writer{store: s, version: version, dir: dir, staged: staged, held: held}, nil
}

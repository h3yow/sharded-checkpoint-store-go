// Package checkpoint 提供本地分片检查点存储：
// 以本地磁盘模拟并行存储的落盘与恢复过程。
package checkpoint

import "errors"

// 可区分的失败原因。
var (
	// ErrIncompleteCommit 表示检查点未完整提交，不可作为最新版本加载。
	ErrIncompleteCommit = errors.New("checkpoint: incomplete commit")
	// ErrShardMissing 表示分片文件缺失。
	ErrShardMissing = errors.New("checkpoint: shard missing")
	// ErrShardTruncated 表示分片被截断（长度不足）。
	ErrShardTruncated = errors.New("checkpoint: shard truncated")
	// ErrShardCorrupted 表示分片校验失败（魔数/校验和不匹配）。
	ErrShardCorrupted = errors.New("checkpoint: shard corrupted")
	// ErrVersionExists 表示重复提交或覆盖已提交版本。
	ErrVersionExists = errors.New("checkpoint: version already committed")
	// ErrVersionRegression 表示版本号回退。
	ErrVersionRegression = errors.New("checkpoint: version regression")
	// ErrUnsupportedFormat 表示格式版本超出支持范围。
	ErrUnsupportedFormat = errors.New("checkpoint: unsupported format version")
	// ErrResourceLimit 表示超出配置的资源上限。
	ErrResourceLimit = errors.New("checkpoint: resource limit exceeded")
	// ErrNoCheckpoint 表示存储中没有任何已提交检查点。
	ErrNoCheckpoint = errors.New("checkpoint: no committed checkpoint")
	// ErrInvalidVersion 表示版本编号本身不合法（如 0，无法通过恢复入口寻址）。
	ErrInvalidVersion = errors.New("checkpoint: invalid version")
)

// 格式版本兼容范围。
const (
	MinFormatVersion = 1
	MaxFormatVersion = 2
)

// StagingPolicy 决定写入过程中暂存数据达到上限时的行为。
type StagingPolicy int

const (
	// StagingReject 暂存超限立即拒绝，返回 ErrResourceLimit。
	StagingReject StagingPolicy = iota
	// StagingWait 暂存超限时阻塞等待空位，直到有容量或 ctx 取消。
	StagingWait
)

// Config 为存储的可配置资源上限与行为策略。
type Config struct {
	// MaxShards 单个检查点允许的最大分片数。
	MaxShards int
	// MaxShardBytes 单个分片允许的最大字节数。
	MaxShardBytes int64
	// MaxInflight 允许并行进行的写入事务数，超限时阻塞。
	MaxInflight int
	// MaxStagedBytes 所有进行中的写入事务在内存中暂存的数据总量上限（字节）。
	// <= 0 表示不限制。暂存数据在 Commit 落盘、Abort/失败时释放。
	MaxStagedBytes int64
	// Staging 暂存超限时的策略，零值为 StagingReject。
	Staging StagingPolicy
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.MaxShards <= 0 {
		out.MaxShards = 64
	}
	if out.MaxShardBytes <= 0 {
		out.MaxShardBytes = 64 << 20
	}
	if out.MaxInflight <= 0 {
		out.MaxInflight = 4
	}
	return out
}

// ShardMeta 描述一个分片在清单中的元数据。
type ShardMeta struct {
	Index  int    `json:"index"`
	Size   int64  `json:"size"`
	CRC32  uint32 `json:"crc32,omitempty"` // 格式 v2 起提供；v1 缺省为 0
	Chunks int    `json:"chunks"`          // v1 起提供
}

// Manifest 是一次提交的清单，落盘为 manifest.json（原子重命名即提交点）。
type Manifest struct {
	FormatVersion int         `json:"format_version"`
	Version       uint64      `json:"version"`
	ShardCount    int         `json:"shard_count"`
	Shards        []ShardMeta `json:"shards"`
}

// ShardReport 报告单个分片的完整性判定结果。
type ShardReport struct {
	Index int
	OK    bool
	Err   error // 缺失/截断/损坏等可区分原因
}

// RecoveryReport 是恢复一次已提交检查点后的完整性报告。
type RecoveryReport struct {
	Version uint64
	Format  int
	Shards  []ShardReport
}

// OK 报告全部分片是否完整。
func (r *RecoveryReport) OK() bool {
	for _, s := range r.Shards {
		if !s.OK {
			return false
		}
	}
	return true
}

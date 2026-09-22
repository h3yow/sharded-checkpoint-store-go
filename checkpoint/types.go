// Package checkpoint 提供本地分片检查点存储：
// 以本地磁盘模拟并行存储的落盘与恢复过程。
package checkpoint

import "time"

// FormatVersion 检查点清单（manifest）的格式版本。
type FormatVersion uint32

const (
	// FormatV1 首个格式版本：包含分片数量、大小与校验和。
	FormatV1 FormatVersion = 1
	// FormatV2 增加 SourceVersion（追加来源版本）与 CreatedAt 字段。
	FormatV2 FormatVersion = 2
	// CurrentFormat 当前写入使用的格式版本。
	CurrentFormat = FormatV2
	// MinSupportedFormat 可读取的最旧格式版本。
	MinSupportedFormat = FormatV1
)

// BackpressurePolicy 资源超限时的策略。
type BackpressurePolicy int

const (
	// Reject 超限立即返回错误。
	Reject BackpressurePolicy = iota
	// Block 超限阻塞等待直到有配额或被取消。
	Block
)

// Options 存储配置与资源上限。
type Options struct {
	// MaxShards 单个检查点允许的最大分片数，0 表示不限制。
	MaxShards int
	// MaxShardBytes 单个分片允许的最大字节数，0 表示不限制。
	MaxShardBytes int64
	// MaxBufferedBytes 单个写入会话允许缓冲的最大字节数，0 表示不限制。
	MaxBufferedBytes int64
	// MaxInflightCommits 同时进行中的提交数上限，0 表示不限制。
	MaxInflightCommits int
	// Policy 超限策略，默认 Reject。
	Policy BackpressurePolicy
}

// ShardMeta 单个分片的元数据。
type ShardMeta struct {
	Index  int    `json:"index"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"` // hex 编码
}

// Manifest 描述一个完整提交的检查点。
type Manifest struct {
	Format  FormatVersion `json:"format"`
	Version uint64        `json:"version"`
	// SourceVersion 本版本基于哪个已提交版本追加而来，0 表示全新检查点。
	// V1 格式无此字段，解码时按 0 处理（语义确定）。
	SourceVersion uint64      `json:"source_version,omitempty"`
	CreatedAt     time.Time   `json:"created_at,omitempty"`
	Shards        []ShardMeta `json:"shards"`
}

// TotalBytes 返回所有分片字节数之和。
func (m *Manifest) TotalBytes() int64 {
	var n int64
	for _, s := range m.Shards {
		n += s.Size
	}
	return n
}

// ShardFailure 描述单个分片的校验失败原因。
type ShardFailure struct {
	Index  int
	Reason ShardFailureReason
	Detail string
}

// ShardFailureReason 可区分的分片失败原因。
type ShardFailureReason int

const (
	ShardMissing    ShardFailureReason = iota // 分片文件缺失
	ShardTruncated                            // 分片大小与清单不符（截断或超长）
	ShardCorrupted                            // 校验和不匹配
	ShardOrphan                               // 磁盘上存在但清单未声明
)

func (r ShardFailureReason) String() string {
	switch r {
	case ShardMissing:
		return "missing"
	case ShardTruncated:
		return "truncated"
	case ShardCorrupted:
		return "corrupted"
	case ShardOrphan:
		return "orphan"
	}
	return "unknown"
}

// IntegrityReport 恢复时对检查点分片完整性的报告。
type IntegrityReport struct {
	Version  uint64
	Complete bool
	Failures []ShardFailure
}

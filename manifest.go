package checkpoint

import (
	"encoding/json"
	"fmt"
)

// encodeManifest 将清单序列化为 JSON 字节。
func encodeManifest(m *Manifest) ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: encode manifest: %w", err)
	}
	return data, nil
}

// decodeManifest 解析清单并做确定性校验：
// 格式版本超出 [MinFormatVersion, MaxFormatVersion] 返回 ErrUnsupportedFormat；
// 旧版本缺失字段按既定缺省规则填充（v1 无分片 CRC32，缺省为 0，
// 恢复时退化为仅依赖分片文件内部校验，结论仍然确定）。
func decodeManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: manifest not valid JSON: %v", ErrShardCorrupted, err)
	}
	if m.FormatVersion < MinFormatVersion || m.FormatVersion > MaxFormatVersion {
		return nil, fmt.Errorf("%w: format %d not in [%d, %d]",
			ErrUnsupportedFormat, m.FormatVersion, MinFormatVersion, MaxFormatVersion)
	}
	if m.ShardCount != len(m.Shards) {
		return nil, fmt.Errorf("%w: manifest declares %d shards, lists %d",
			ErrShardCorrupted, m.ShardCount, len(m.Shards))
	}
	for i, sh := range m.Shards {
		if sh.Index != i {
			return nil, fmt.Errorf("%w: manifest shard %d at position %d, indices must be contiguous",
				ErrShardCorrupted, sh.Index, i)
		}
		if sh.Size <= 0 {
			return nil, fmt.Errorf("%w: manifest shard %d has non-positive size %d",
				ErrShardCorrupted, sh.Index, sh.Size)
		}
	}
	return &m, nil
}

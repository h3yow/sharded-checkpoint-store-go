package checkpoint

import (
	"encoding/json"
	"fmt"
)

// manifestV1 用于解析旧格式：V1 没有 SourceVersion/CreatedAt 字段。
// JSON 解码时缺失字段自然为零值，这里显式建模以保证语义确定。
type manifestV1 struct {
	Format  FormatVersion `json:"format"`
	Version uint64        `json:"version"`
	Shards  []ShardMeta   `json:"shards"`
}

// encodeManifest 将清单序列化为字节（始终使用当前格式）。
func encodeManifest(m *Manifest) ([]byte, error) {
	m.Format = CurrentFormat
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("checkpoint: encode manifest: %w", err)
	}
	return data, nil
}

// decodeManifest 解析清单字节，并做格式版本兼容检查。
// 旧版本（V1）缺失的字段按确定的默认值处理（SourceVersion=0、CreatedAt 零值），
// 不会导致恢复结论不确定；超出支持范围的版本返回 ErrUnsupportedFormat。
func decodeManifest(data []byte) (*Manifest, error) {
	var head struct {
		Format FormatVersion `json:"format"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return nil, fmt.Errorf("checkpoint: decode manifest header: %w", err)
	}
	if head.Format < MinSupportedFormat || head.Format > CurrentFormat {
		return nil, fmt.Errorf("%w: format=%d supported=[%d,%d]",
			ErrUnsupportedFormat, head.Format, MinSupportedFormat, CurrentFormat)
	}

	m := &Manifest{Format: head.Format}
	switch head.Format {
	case FormatV1:
		var v1 manifestV1
		if err := json.Unmarshal(data, &v1); err != nil {
			return nil, fmt.Errorf("checkpoint: decode v1 manifest: %w", err)
		}
		m.Version = v1.Version
		m.Shards = v1.Shards
		// SourceVersion/CreatedAt 保持零值：V1 无此概念，语义确定为“全新检查点”。
	default: // FormatV2 及以后（当前范围内）
		if err := json.Unmarshal(data, m); err != nil {
			return nil, fmt.Errorf("checkpoint: decode v2 manifest: %w", err)
		}
	}

	if err := validateManifest(m); err != nil {
		return nil, err
	}
	return m, nil
}

// validateManifest 校验清单内部一致性：分片索引连续、大小非负。
func validateManifest(m *Manifest) error {
	for i, s := range m.Shards {
		if s.Index != i {
			return fmt.Errorf("checkpoint: manifest shard index gap: position=%d index=%d", i, s.Index)
		}
		if s.Size < 0 {
			return fmt.Errorf("checkpoint: manifest shard %d has negative size %d", i, s.Size)
		}
	}
	return nil
}

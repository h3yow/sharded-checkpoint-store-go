package checkpoint

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// 分片文件二进制布局（小端）：
//
//	magic(4) | formatVersion(2) | index(4) | payloadLen(8) | payload | crc32(payload)(4)
var shardMagic = [4]byte{'S', 'C', 'K', 'P'}

const shardHeaderLen = 4 + 2 + 4 + 8
const shardTrailerLen = 4

// encodeShard 将 payload 编码为分片文件字节流。
func encodeShard(format, index int, payload []byte) []byte {
	buf := make([]byte, 0, shardHeaderLen+len(payload)+shardTrailerLen)
	buf = append(buf, shardMagic[:]...)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(format))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(index))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(len(payload)))
	buf = append(buf, payload...)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.ChecksumIEEE(payload))
	return buf
}

// decodeShard 从 r 读取并校验一个分片，返回 payload 与其元数据。
// 失败原因可区分为 ErrShardTruncated / ErrShardCorrupted。
func decodeShard(r io.Reader) (meta ShardMeta, payload []byte, err error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return meta, nil, fmt.Errorf("checkpoint: read shard: %w", err)
	}
	if len(data) < shardHeaderLen {
		return meta, nil, fmt.Errorf("%w: header need %d bytes, got %d",
			ErrShardTruncated, shardHeaderLen, len(data))
	}
	if !bytes.Equal(data[:4], shardMagic[:]) {
		return meta, nil, fmt.Errorf("%w: bad magic %q", ErrShardCorrupted, data[:4])
	}
	format := int(binary.LittleEndian.Uint16(data[4:6]))
	index := int(binary.LittleEndian.Uint32(data[6:10]))
	payloadLen := binary.LittleEndian.Uint64(data[10:18])
	want := shardHeaderLen + int(payloadLen) + shardTrailerLen
	if uint64(len(data)) < uint64(want) {
		return meta, nil, fmt.Errorf("%w: shard %d need %d bytes, got %d",
			ErrShardTruncated, index, want, len(data))
	}
	if uint64(len(data)) > uint64(want) {
		return meta, nil, fmt.Errorf("%w: shard %d has %d trailing bytes",
			ErrShardCorrupted, index, uint64(len(data))-uint64(want))
	}
	payload = data[shardHeaderLen : shardHeaderLen+int(payloadLen)]
	crc := binary.LittleEndian.Uint32(data[want-shardTrailerLen:])
	if crc32.ChecksumIEEE(payload) != crc {
		return meta, nil, fmt.Errorf("%w: shard %d crc mismatch", ErrShardCorrupted, index)
	}
	meta = ShardMeta{Index: index, Size: int64(len(data)), CRC32: crc, Chunks: 1}
	_ = format // 分片头格式版本当前仅作记录，兼容性由清单版本统一判定
	return meta, payload, nil
}

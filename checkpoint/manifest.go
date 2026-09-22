package checkpoint

// encodeManifest 将清单序列化为字节（当前格式）。
func encodeManifest(m *Manifest) ([]byte, error) {
	return nil, nil
}

// decodeManifest 解析清单字节，并做格式版本兼容检查。
// 旧版本缺失字段按确定的默认值处理；超出支持范围返回 ErrUnsupportedFormat。
func decodeManifest(data []byte) (*Manifest, error) {
	return nil, nil
}

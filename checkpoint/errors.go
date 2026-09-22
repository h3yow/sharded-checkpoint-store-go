package checkpoint

import "errors"

var (
	// ErrVersionExists 重复提交同一版本或试图覆盖已提交版本。
	ErrVersionExists = errors.New("checkpoint: version already committed")
	// ErrVersionRegression 版本号回退（小于等于最新已提交版本）。
	ErrVersionRegression = errors.New("checkpoint: version regression")
	// ErrIncompleteCommit 检查点未完整提交，不可作为最新版本加载。
	ErrIncompleteCommit = errors.New("checkpoint: incomplete commit")
	// ErrNoCheckpoint 没有任何已提交的检查点。
	ErrNoCheckpoint = errors.New("checkpoint: no committed checkpoint")
	// ErrUnsupportedFormat 格式版本超出支持范围。
	ErrUnsupportedFormat = errors.New("checkpoint: unsupported format version")
	// ErrShardIntegrity 分片完整性校验失败（缺失/截断/损坏/孤儿）。
	ErrShardIntegrity = errors.New("checkpoint: shard integrity failure")
	// ErrLimitExceeded 资源上限被触发且策略为拒绝。
	ErrLimitExceeded = errors.New("checkpoint: resource limit exceeded")
	// ErrWriterClosed 写入会话已关闭（已提交或已中止）。
	ErrWriterClosed = errors.New("checkpoint: writer already closed")
)

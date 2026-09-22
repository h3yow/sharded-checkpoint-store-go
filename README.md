# sharded-checkpoint-store-go

本地分片检查点存储：用本地磁盘模拟并行存储的落盘与恢复过程，保证写入、快照与恢复在进程中断、并发访问和格式演进下的最终状态可预期。

## 目录与文件格式

```
<root>/
  chk-<version>/            # 一个检查点版本一个目录
    shard-00000.ckpt        # 分片文件
    shard-00001.ckpt
    manifest.tmp            # 提交前的临时清单（出现即表示未提交）
    manifest.json           # 清单，原子重命名落位 = 提交点
```

### 分片文件二进制布局（小端）

| 字段 | 长度（字节） | 说明 |
|---|---|---|
| magic | 4 | 固定 `SCKP` |
| formatVersion | 2 | 分片头格式版本 |
| index | 4 | 分片序号（从 0 连续编号） |
| payloadLen | 8 | 负载长度 |
| payload | payloadLen | 负载 |
| crc32 | 4 | 负载的 CRC32(IEEE) |

### 清单（manifest.json）

```json
{
  "format_version": 2,
  "version": 7,
  "shard_count": 2,
  "shards": [{"index": 0, "size": 31, "crc32": 3079705788, "chunks": 1}]
}
```

## 提交与恢复语义

- **原子提交**：先写入并 fsync 全部分片，再写 `manifest.tmp` 并 fsync，最后 `rename` 为
  `manifest.json`。**重命名是唯一提交点**。此前任何时刻进程中断，目录中没有清单，
  恢复时判定为 `ErrIncompleteCommit`，绝不会被当作最新版本加载。
- **版本规则**：版本号单调递增。重复提交已提交版本返回 `ErrVersionExists`，
  版本号回退返回 `ErrVersionRegression`；同一版本同时只允许一个写入事务。
- **恢复**：`Recover(0)` 恢复最新已提交版本；`Recover(v)` 恢复指定版本。
  逐分片校验并返回 `RecoveryReport`，缺失（`ErrShardMissing`）、截断
  （`ErrShardTruncated`）、损坏（`ErrShardCorrupted`）分别报告，绝不静默跳过。
- **中止**：`Abort` 删除整个未提交目录，不残留任何可被误认为有效的内容。
- **并发**：`Store` 内部加锁且清单落盘走原子重命名；多 goroutine 并行
  Begin/WriteShard/Commit/Recover 不会出现半写入可见、分片错位或中间态读取
  （`go test -race` 验证）。

## 格式兼容范围

- 支持 `format_version` ∈ **[1, 2]**（`MinFormatVersion`/`MaxFormatVersion`）。
- **v1**：清单无分片级 `crc32` 字段，读取时缺省为 0，恢复确定性地退化为仅依赖
  分片文件内部 CRC 校验——缺失字段不会导致恢复结论不确定。
- **v2**：清单携带分片级 `crc32`，恢复时与分片实际校验和交叉核对。
- 超出范围的版本（如 99）明确返回 `ErrUnsupportedFormat`。

## 资源上限

`Config` 可配置：`MaxShards`（单检查点最大分片数）、`MaxShardBytes`（单分片最大字节）、
`MaxInflight`（最大并行写入事务数，超限阻塞直至有空位或 ctx 取消）。
分片超限返回 `ErrResourceLimit`；配合 `Abort` 清理，失败后无残留。

## 验证方法

```sh
go test ./...        # 全部单元测试
go test -race ./...  # 竞态检测
go test -v ./...     # 查看关键输入与判定依据日志
```

测试覆盖：中断写入（`TestInterruptedWriteNotVisible`）、中止清理
（`TestAbortLeavesNoResidue`）、分片缺失/截断/损坏（`TestShardIntegrityFailures`）、
版本重复/回退（`TestVersionRules`）、资源上限（`TestResourceLimits`）、
并发提交与并发读写交错（`TestConcurrentCommits`/`TestConcurrentRecoverDuringCommit`）、
格式兼容与超范围拒绝（`TestFormatCompatibility`）、清单损坏（`TestCorruptManifest`）。

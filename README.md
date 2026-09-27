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

- **原子提交**：分片先暂存在内存，`Commit` 时统一落盘并 fsync，再写
  `manifest.tmp` 并 fsync，最后 `rename` 为 `manifest.json`。**重命名是唯一提交点**。
  此前任何时刻进程中断，目录中没有清单，恢复时判定为 `ErrIncompleteCommit`，
  绝不会被当作最新版本加载。
- **版本准入**：版本号单调递增，准入校验发生在两处——
  - `Begin`/`BeginFrom` 时：版本 `0` 被 `Recover(0)`（最新版本）语义占用、
    提交后将无法按编号读回，直接返回 `ErrInvalidVersion`；重复或覆盖已提交版本
    返回 `ErrVersionExists`；版本号回退返回 `ErrVersionRegression`；
    同一版本同时只允许一个写入事务。
  - `Commit` 时（持锁复核）：若本事务准备期间已有更高版本提交成功，
    本版本不得再落盘，临时目录被清理并返回 `ErrVersionRegression`——
    存储中不会出现“版本顺序与提交结果对不上”的残留。
- **恢复**：`Recover(0)` 恢复最新已提交版本；`Recover(v)` 恢复指定版本。
  逐分片校验并返回 `RecoveryReport`，缺失（`ErrShardMissing`）、截断
  （`ErrShardTruncated`）、损坏（`ErrShardCorrupted`）分别报告，绝不静默跳过。
- **中止**：`Abort` 删除整个未提交目录，不残留任何可被误认为有效的内容。
- **并发**：`Store` 内部加锁且清单落盘走原子重命名；多 goroutine 并行
  Begin/WriteShard/Commit/Recover 不会出现半写入可见、分片错位或中间态读取
  （`go test -race` 验证）。

## 版本承接（派生提交）

`BeginFrom(ctx, newVersion, baseVersion)` 开启派生写入事务：新版本承接
`baseVersion`（`0` 表示最新已提交版本）的全部分片内容，之后用 `WriteShard`
覆盖或追加部分分片即可，`Commit` 后的恢复结果与“承接内容 + 本次改动”的
全量重写完全一致；来源版本不受影响。

- **失败先于落盘**：来源版本不存在（`ErrNoCheckpoint`）、未完整提交
  （`ErrIncompleteCommit`）或任何分片读不出来（如 `ErrShardCorrupted`）时，
  派生在新数据落盘之前失败，不会留下看似有效的新版本目录。
- **兼容范围**：来源可以是 [1, 2] 中任意格式版本。v1 来源缺失分片级
  `crc32` 字段时按既定缺省规则（置 0、退化为分片内部校验）确定性读取；
  派生出的新版本一律以当前格式（v2）落盘。

## 暂存容量控制

写入事务的分片先暂存在内存，`Config.MaxStagedBytes` 限制**所有进行中事务**
暂存数据的总量（`<= 0` 表示不限）。`Config.Staging` 选择超限策略：

- **`StagingReject`（默认）**：超限立即返回 `ErrResourceLimit`。
- **`StagingWait`**：阻塞等待空位；其他事务 `Commit`/`Abort` 释放配额后被唤醒。
  等待中 `ctx` 取消会尽快退出并返回 `ctx.Err()`。

覆盖同序号分片按差额增减配额；单笔超过总容量的写入永远无法满足，两种策略都
立即拒绝。无论拒绝还是取消，暂存数据尚未落盘，`Abort`/失败路径会清理目录并
归还配额，不会留下会被恢复误判为有效版本的内容。

## 格式兼容范围

- 支持 `format_version` ∈ **[1, 2]**（`MinFormatVersion`/`MaxFormatVersion`）。
- **v1**：清单无分片级 `crc32` 字段，读取时缺省为 0，恢复确定性地退化为仅依赖
  分片文件内部 CRC 校验——缺失字段不会导致恢复结论不确定。
- **v2**：清单携带分片级 `crc32`，恢复时与分片实际校验和交叉核对。
- 超出范围的版本（如 99）明确返回 `ErrUnsupportedFormat`。

## 资源上限

`Config` 可配置：`MaxShards`（单检查点最大分片数）、`MaxShardBytes`（单分片最大字节）、
`MaxInflight`（最大并行写入事务数，超限阻塞直至有空位或 ctx 取消）、
`MaxStagedBytes` + `Staging`（暂存总量上限与超限策略，见上节）。
分片或暂存超限返回 `ErrResourceLimit`；配合 `Abort` 清理，失败后无残留。

## 验证方法

```sh
go test ./...        # 全部单元测试
go test -race ./...  # 竞态检测
go test -v ./...     # 查看关键输入与判定依据日志

# 单独复现新增行为
go test -v -run 'TestDerive' ./...        # 版本承接：与全量重写一致 / 来源失败无残留 / 老格式来源
go test -v -run 'TestStaging' ./...       # 暂存超限：拒绝 / 等待取消 / 唤醒
go test -v -run 'TestLowVersion|TestConcurrentOutOfOrder|TestVersionZero' ./...  # 版本准入竞争
```

测试覆盖：中断写入（`TestInterruptedWriteNotVisible`）、中止清理
（`TestAbortLeavesNoResidue`）、分片缺失/截断/损坏（`TestShardIntegrityFailures`）、
版本重复/回退（`TestVersionRules`/`TestDuplicateAndOverwriteRejected`）、
资源上限（`TestResourceLimits`）、并发提交与并发读写交错
（`TestConcurrentCommits`/`TestConcurrentRecoverDuringCommit`）、
格式兼容与超范围拒绝（`TestFormatCompatibility`）、清单损坏（`TestCorruptManifest`）、
版本承接（`TestDeriveMatchesFullRewrite`/`TestDeriveFromUnavailableBase`/
`TestDeriveFromLegacyFormat`/`TestDeriveAbort`）、
暂存容量（`TestStagingRejectPolicy`/`TestStagingWaitPolicyCancel`/
`TestStagingWaitPolicyUnblocked`/`TestStagingExceedsTotalCapacity`）、
版本准入竞争（`TestLowVersionCommitRejectedAfterHighCommits`/
`TestConcurrentOutOfOrderCommits`/`TestVersionZeroRejected`）。

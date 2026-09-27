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
- **版本规则（准入）**：版本号单调递增。
  - 重复提交/覆盖已提交版本返回 `ErrVersionExists`，版本号回退返回
    `ErrVersionRegression`；同一版本同时只允许一个写入事务。
  - `version == 0` 返回 `ErrInvalidVersion`：`Recover(0)` 语义为“最新版本”，
    v0 一旦提交将永远无法经正常恢复入口读回，因此在开始写入前直接拒绝。
  - 提交时在串行化的提交临界区内**复核**准入：若低版本还在准备时高版本
    已经提交成功，低版本的 `Commit` 返回 `ErrVersionRegression` 并清理目录，
    不会悄悄落盘成功——存储中版本顺序与提交结果始终一致。
- **恢复**：`Recover(0)` 恢复最新已提交版本；`Recover(v)` 恢复指定版本。
  逐分片校验并返回 `RecoveryReport`，缺失（`ErrShardMissing`）、截断
  （`ErrShardTruncated`）、损坏（`ErrShardCorrupted`）分别报告，绝不静默跳过。
- **中止**：`Abort` 删除整个未提交目录，不残留任何可被误认为有效的内容。
- **版本承接（派生）**：`BeginDerive(ctx, version, sourceVersion)` 开启的新事务
  承接来源版本的全部分片内容，之后只需用 `WriteShard` 覆盖/追加需要改动的分片；
  提交后恢复新版本看到的就是“承接内容 + 本次改动”的完整结果，与把旧版本
  全量重写一遍再改动完全等价。来源版本只读、不受任何影响。
  来源版本不存在、未完整提交或读不出来（如分片损坏）时，派生在落任何新数据
  之前失败，不创建新版本目录、不留任何疑似有效版本的残留。
  来源为较老格式版本（v1）时按既有缺省规则确定读取（见“格式兼容范围”），
  派生输出始终以当前格式（v2）落盘。
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

### 暂存容量（MaxStagedBytes + StagingPolicy）

写入事务的分片数据先**暂存在内存**，`Commit` 时才统一落盘。
`MaxStagedBytes` 限制所有进行中事务暂存数据的总量（含派生事务承接的基础分片），
达到上限时按 `StagingPolicy` 处理：

- **`StagingReject`（默认）**：超限写入立即返回 `ErrResourceLimit`。
- **`StagingWait`**：阻塞等待其他事务提交/中止释放空位；等待过程中 ctx 取消
  会尽快退出并返回 `ctx.Err()`。

无论拒绝还是取消，对应事务都未落盘任何分片数据，配合 `Abort` 清理后
不会留下会被恢复误判为有效版本的内容。单个分片超过总量上限时永远无法满足，
两种策略都立即返回 `ErrResourceLimit`（不会死等）。

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
格式兼容与超范围拒绝（`TestFormatCompatibility`）、清单损坏（`TestCorruptManifest`）、
版本承接与全量重写等价（`TestDeriveEqualsFullRewrite`）、
来源不可用失败且无残留（`TestDeriveSourceUnavailable`）、
老格式来源派生（`TestDeriveFromLegacyFormat`）、
暂存拒绝/等待取消/等待放行（`TestStagingReject`/`TestStagingWaitCancel`/`TestStagingWaitUnblocked`）、
高低版本竞争与 v0 拒绝（`TestStaleCommitRejected`/`TestConcurrentHighLowRace`/`TestVersionZeroRejected`）。

本地复现验证：

```sh
go test ./...                  # 全部单元测试
go test -race -count=1 ./...   # 竞态检测（含并发提交/高低版本竞争用例）
go test -v -run TestDerive ./...   # 只看版本承接相关用例
go test -v -run TestStaging ./...  # 只看暂存容量相关用例
```

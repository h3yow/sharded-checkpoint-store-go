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
- **`StagingWait`**：容量不足时进入 FIFO 等待队列阻塞，直到有容量或 `ctx` 取消。
  - **一次释放、批量唤醒**：等待策略为每个排队事务维护**独立的唤醒信号**。任一事务
    `Commit`/`Abort` 释放配额时，会**从队首扫描整个等待队列，把当前空位放得下的
    排队事务一次性全部放行**——一次释放可以同时唤醒多个等待者，信号不会丢失，
    也不会只放行一个。因此不会出现“已经有空闲容量，却只有一个排队事务被放行、
    其余必须再等下一次释放”的批量并发卡死。
  - **不超发、不饿死**：扫描按 FIFO 顺序进行，只有 `已用量 + 申请量 <= 容量`
    的等待者才会拿到配额，放不下的继续留在队列中，且**不会挡住它后面放得下的
    等待者**（小申请可越过大申请先放行）；一旦后续释放让队首等待者放得下，它会
    在最早一次扫描中被放行，不会因持续到达的新申请而长期拿不到已有空位。
    新申请在队列非空时一律入队尾，不得插队抢占。
  - **等待中取消**：无论同时有多少个等待者，任一等待者的 `ctx` 取消都会使其
    **尽快退出等待并返回 `ctx.Err()`**，同时从等待队列摘除、不占任何配额；
    取消不影响其他等待者被后续释放唤醒。唤醒与取消同时发生时，取消语义优先，
    已拿到的配额会被立即归还并继续放行其他等待者，保证配额不泄漏。

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
go test -v -run 'TestStaging' ./...       # 暂存超限：拒绝 / 等待取消 / 单次释放多等待者唤醒
go test -v -run 'TestLowVersion|TestConcurrentOutOfOrder|TestVersionZero' ./...  # 版本准入竞争

# 重点复现“一次释放唤醒多个等待者”（预算级 + Store 级，建议带竞态检测）
go test -race -v -run 'TestStagingBudgetOneReleaseWakesMultipleWaiters|TestStoreMultipleWaitersReleasedTogether' ./...
# 等待中取消（多个等待者并存时取消其中一部分，无残留、不泄漏配额）
go test -race -v -run 'TestStagingBudgetWaitCancel|TestStoreMultipleWaitersCancelNoResidue' ./...
# 放不下继续等待、越过大申请放行、随后不再饿死
go test -race -v -run 'TestStagingBudgetWaiterThatDoesNotFitKeepsWaiting|TestStoreDoesNotFitKeepsWaitingStoreLevel' ./...
# 大批量同批唤醒 + 取消交错的竞态压力（可加 -count=N 反复跑）
go test -race -count=20 -run 'TestStagingWaitMassWakeupRace' ./...
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
暂存等待的批量唤醒与取消边界（`TestStagingBudgetOneReleaseWakesMultipleWaiters`/
`TestStagingBudgetSingleWaiterWoken`/`TestStagingBudgetWaitCancel`/
`TestStagingBudgetWaiterThatDoesNotFitKeepsWaiting`/
`TestStoreMultipleWaitersReleasedTogether`/`TestStoreMultipleWaitersCancelNoResidue`/
`TestStoreDoesNotFitKeepsWaitingStoreLevel`/`TestStagingWaitMassWakeupRace`）、
版本准入竞争（`TestLowVersionCommitRejectedAfterHighCommits`/
`TestConcurrentOutOfOrderCommits`/`TestVersionZeroRejected`）。

### 暂存等待唤醒语义（缺陷背景）

早期实现用一个容量为 1 的共享信号通道承载所有唤醒提示：一次 `release` 最多
投递一个信号。当一次释放出的空间足够满足多个排队事务时，只有其中一个等待者
被唤醒，其余等待者即便空位放得下也必须再等下一次释放；批量并发写入下这会
表现为偶发的长时间卡死。现实现改为**每等待者独立信号 + 释放时全队列扫描
一次性放行所有放得下者**，上述用例在旧实现上会稳定超时失败、在新实现上通过，
可作为本地复现该缺陷的回归基线。

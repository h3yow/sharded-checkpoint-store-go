# sharded-checkpoint-store-go

本地分片检查点存储：用本地磁盘模拟并行存储的落盘与恢复过程，保证写入、快照与恢复在进程中断、并发访问和格式演进下的最终状态可预期。

## 磁盘布局与检查点格式

```
<dir>/
  committed/                     # 已提交检查点（恢复唯一可见的位置）
    v00000000000000000001/
      manifest.json              # 清单（格式见下）
      shards/000000.bin          # 分片数据，按 6 位序号命名
  staging/                       # 写入中的临时区，恢复时永不可见
    w-<pid>-<seq>/...
```

清单（manifest）为 JSON，当前格式版本 **V2**：

```json
{
  "format": 2,
  "version": 3,
  "source_version": 2,           // V2 新增：追加来源版本，0 表示全新检查点
  "created_at": "2026-09-22T...",// V2 新增
  "shards": [
    {"index": 0, "size": 128, "sha256": "<hex>"}
  ]
}
```

分片索引必须从 0 连续编号，清单中每个分片记录字节数与 SHA-256。

## 提交与恢复语义

### 提交（两阶段，原子发布）

1. `Begin(ctx, version, base)` 校验版本并创建 staging 目录；`base>0` 时把基线版本的全部分片复制进 staging 作为追加起点。
2. `WriteShard(index, r, size)` 顺序写入分片（index 必须递增连续），写后 fsync。
3. `Commit()` 在持锁状态下**再次**校验版本，然后将 staging 目录整体 `rename` 到 `committed/v<version>`——rename 的原子性保证检查点要么完整可见、要么完全不可见，不存在半写入可见。
4. `Abort()` 删除 staging，不留任何可见状态。

版本规则（在 Begin 与 Commit 两处都校验，防止竞争）：

- 重复提交同一版本 → `ErrVersionExists`
- 版本号小于最新已提交版本（回退）→ `ErrVersionRegression`
- 提交失败时 staging 被清理，不会残留被误认为有效的不完整检查点

### 恢复

- `Latest()` / `Recover(0)` 只扫描 `committed/` 下含 `manifest.json` 的目录；staging 残留、中断的写入对恢复完全不可见，未完整提交的检查点报 `ErrIncompleteCommit` 而**不会**被当作最新版本加载。
- `Recover(version)` 对照清单逐分片校验，返回 `IntegrityReport`，失败原因可区分：

  | 原因 | 含义 |
  |---|---|
  | `missing` | 分片文件缺失 |
  | `truncated` | 实际大小与清单不符（截断/超长） |
  | `corrupted` | SHA-256 校验和不匹配 |
  | `orphan` | 磁盘上存在但清单未声明的文件 |

  任何失败都使 `Complete=false` 并返回 `ErrShardIntegrity`，绝不静默跳过。

## 格式兼容范围

- 支持读取的格式版本：`[1, 2]`（`MinSupportedFormat` ~ `CurrentFormat`）。
- **V1** 清单无 `source_version`/`created_at` 字段，解码时按确定默认值处理（`SourceVersion=0`，即“全新检查点”语义），恢复结论不因缺字段而不确定。
- 超出范围的版本（如 0 或 99）→ `ErrUnsupportedFormat`，明确拒绝。
- 写入始终使用当前格式 V2。

## 并发安全

- 每个写入会话使用独立的 staging 目录，会话间互不影响；`Writer` 本身非并发安全。
- 版本判定与提交发布在互斥锁临界区内完成；多会话竞争同一版本时恰好一个成功，其余收到 `ErrVersionExists`。
- 恢复只读取 `committed/` 中已原子发布的目录，并发写入期间不会读到中间态。
- 全部测试在 `-race` 下通过。

## 资源上限与背压

`Options` 支持可配置上限，超限按策略处理：

| 配置 | 含义 |
|---|---|
| `MaxShards` | 单检查点最大分片数 |
| `MaxShardBytes` | 单分片最大字节数 |
| `MaxBufferedBytes` | 单会话累计缓冲字节数 |
| `MaxInflightCommits` | 同时进行中的写入会话数 |
| `Policy` | `Reject`（默认，返回 `ErrLimitExceeded`）或 `Block`（阻塞等待配额，可被 ctx 取消） |

超限失败后 staging 数据对恢复不可见，重新打开存储不会发现任何"有效"的不完整检查点。

## 验证方法

```bash
go build ./...
go test ./checkpoint/ -count=1        # 功能测试
go test ./checkpoint/ -race -count=1  # 并发竞态检测
go test ./checkpoint/ -v -run TestShardFailures  # 查看关键输入与判定依据日志
```

测试覆盖：

- **中断写入**：会话未 Commit 即丢弃（模拟崩溃），重启后该版本不可见（`TestInterruptedWriteNotVisible`）
- **分片损坏**：缺失/截断/篡改/孤儿四种故障分别注入，验证可区分的失败原因（`TestShardFailuresAreDistinguishable`）
- **并发提交**：多 goroutine 竞争同一/不同版本，验证恰好一个成功、无分片错位、恢复无中间态（`concurrency_test.go`）
- **版本不兼容**：V1 可读且默认值确定，超范围版本明确拒绝（`compat_test.go`）
- **资源上限**：四类上限的拒绝与阻塞策略，失败后无残留（`limits_test.go`）

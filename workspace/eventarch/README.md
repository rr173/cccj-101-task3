# eventarch — 多来源设备遥测事件归档服务

面向多来源设备遥测的持久化事件归档服务。来源可能离线数小时后补传；接收端区分
**迟到（late）**、**重复（duplicate）**、**时钟回拨（clock_rollback）**，在不破坏
每台设备业务顺序的前提下持续形成可查询的不可变分段；运维人员可冻结某一时刻的视图
并从中发起回放，回放期间新数据照常写入但不会混入已冻结批次。写入确认（ACK）前完成
WAL fsync，节点重启不丢已确认数据；分段损坏时自动隔离并给出可继续处理的位置。

零第三方依赖（Python 3.11 标准库），单容器即可运行。

## 快速开始

### 容器（推荐）

```bash
docker build -t eventarch:0.1.0 .
docker run --rm -p 8080:8080 -v eventarch-data:/data eventarch:0.1.0
```

或：

```bash
docker compose up --build
```

### 本地（Python 3.11+）

```bash
EA_DATA_DIR=./data python3 -m eventarch.server
# 或 make run
```

### 验证

```bash
make test     # 单元测试（分类/持久化/冻结/损坏隔离）
make smoke    # 端到端：起服务→分类→冻结→重启→损坏→隔离→重建
```

## 事件模型与分类语义

写入事件（`POST /v1/ingest`）：

```json
{
  "events": [
    {
      "device_id": "dev-A",                 // 必填，设备标识
      "event_id": "01J…",                   // 必填，幂等键（来源侧唯一）
      "seq": 12345,                         // 必填，设备侧业务序号（单调）
      "device_ts": "2026-09-17T08:00:00Z",  // 必填，设备时钟
      "payload": { "temp": 21.5 }           // 任意 JSON
    }
  ]
}
```

每个事件返回 `{event_id, status, offset, flags}`：

| 标记 | 判定 | 处理 |
|---|---|---|
| `duplicate` | `(device_id, event_id)` 已存在（含批内重复） | **不重复写入**，返回原 offset，幂等 ACK |
| `late` | `now - device_ts > EA_LATE_THRESHOLD_SEC`（离线补传） | 正常入库，打标 |
| `clock_rollback` | `device_ts` 小于该设备已见最大设备时钟 | 正常入库，打标 |
| `seq_conflict` | `seq` 已出现但 `event_id` 不同（序号复用冲突） | 正常入库，打标 |

- **业务顺序**：分段按到达顺序物理追加（不可变）；每台设备另维护按
  `(seq, offset)` 排序的索引。迟到的补传数据进入当前开放段并打标，绝不回写
  已封存段，但设备查询仍按业务序号有序返回。
- **offset**：全局单调的接收序号，是冻结、回放、续传的统一游标。

## 核心 API

| 方法/路径 | 说明 |
|---|---|
| `POST /v1/ingest` | 批量写入（批末一次 fsync 后 ACK）；单项错误不拖垮整批 |
| `GET /v1/devices` | 设备清单（事件数、最大序号、最大设备时钟） |
| `GET /v1/devices/{id}/events?from_seq=&from_offset=&limit=` | 单设备**业务序**查询，返回 `events/gaps/next` 游标 |
| `GET /v1/segments` | 分段清单（状态、offset 区间、sha256）+ 开放段信息 |
| `GET /v1/segments/{id}/events?from_offset=&limit=` | 段内扫描（逐帧 CRC 校验） |
| `POST /v1/segments/{id}/rebuild` | 提交**后台修复作业**重建被隔离段（立即返回 `202 + job`，不阻塞前台） |
| `GET /v1/repairs?limit=` | 修复作业列表（含状态/尝试次数/阶段/错误） |
| `GET /v1/repairs/{job_id}` | 查询单个修复作业状态 |
| `POST /v1/repairs/{job_id}` | 等待作业到达终态（body 可带 `timeout` 秒） |
| `POST /v1/freeze` | 冻结当前视图：封存开放段，返回 `{id, end_offset, segments}` |
| `GET /v1/freezes` | 冻结列表 |
| `GET /v1/replay?freeze_id=&from_offset=&device_id=&limit=` | 回放冻结视图（不传 `freeze_id` 则回放到当前头） |
| `GET /v1/stats` · `GET /v1/healthz` | 运行指标 / 健康检查 |

### 冻结与回放

```bash
# 1. 冻结：返回 end_offset（排他视界），此后到达的数据进新段
curl -XPOST localhost:8080/v1/freeze -d '{"note":"nightly"}'

# 2. 回放冻结视图（分页：用 next_from_offset 续拉）
curl 'localhost:8080/v1/replay?freeze_id=frz-…&limit=1000'

# 3. 追平后从冻结视界继续消费新数据
curl 'localhost:8080/v1/replay?from_offset=<end_offset>'
```

回放只读取冻结时刻已封存的不可变段；回放期间新到数据写入新的开放段，
**不可能混入已冻结批次**。段被隔离时回放不中断：响应的 `gaps[]` 标注
`{segment, reason, resume_offset}`，流自动从下一健康段继续。

### 损坏隔离与续处理位置

- **启动校验**：逐段 sha256 比对，失败即隔离（`status=quarantined`）。
- **读时校验**：每次读取逐帧 CRC32；发现损坏立即隔离并持久化。
- **隔离影响范围**：被隔离段从查询/回放中剔除，响应携带
  `resume_offset = last_offset + 1`——即可继续处理的位置。
- **修复（后台作业，不独占服务）**：`POST /v1/segments/{id}/rebuild` 不做同步重活，
  只把一个修复作业入队并立即返回 `202 {"job": {...}}`。重型 I/O（扫描保留的
  WAL、写候选段）全部在有界后台工作池中、**在全局锁之外**完成；前台摄取、检索、
  冻结/回放始终响应。同一段已有活动作业时返回同一作业（幂等去重，绝不重复修复）。

```bash
curl -XPOST localhost:8080/v1/segments/seg-…/rebuild        # -> 202 + job id
curl localhost:8080/v1/repairs/job-…                         # 轮询状态
curl -XPOST localhost:8080/v1/repairs/job-… -d '{"timeout":30}'  # 阻塞等待终态
```

修复作业状态机：`queued → running(gathering_wal/staging/committing) →
succeeded | failed`，每次状态迁移落盘到 `state/repairs.json`。失败时
`error.type` 可能是 `wal_coverage_gone`（带 `resume_offset`，调用方可跳过该段）、
`conflict`（重试用尽）、`not_found`、`shutting_down` 等。

### 并发安全与不变量

修复与“日志淘汰 / 同一归档单元状态变化 / 服务重启”三类竞态都显式处理：

- **与日志淘汰竞态**：作业在计划阶段把目标段登记为活动修复，`_wal_keep_from`
  会把它钉在保留水位之内，收集器因此不可能删掉作业即将使用的 WAL；跨轮转
  读到撕裂尾时整体重试（作业幂等）。
- **与同一单元状态变化竞态（乐观版本 CAS）**：每段 meta 带单调 `version`。
  作业计划时快照版本，提交时仅短持锁做 CAS：版本被竞争者改变则回滚文件、
  按当前状态重新计划并重试（有界退避）；若已被同样字节修复则识别为无操作，
  被其它内容取代则安全放弃——**绝不覆盖无损文件**。
- **无损文件绝不原地改写**：候选段写入独立的 `stage-<job>-<n>/<seg>/` 目录并
  自校验 sha256；提交是两次同文件系统内 `rename`（live→`bak-…`、stage→live）
  + 目录 fsync，再原子提交 manifest，最后删除 bak。进程内提交失败会反向
  交换回滚；进程崩溃由启动对账按作业日志完成或回滚。
- **不丢确认数据 / 不产生重复记录**：重建范围严格按
  `[first_offset, last_offset]` 连续对齐且条数一致才提交；设备索引在锁内
  按段整体替换（先剔除该段旧条目再装入新索引），不会重复插入。
- **消费游标与快照视界不变**：修复只换段的字节与 sha256/版本，段的
  offset 区间、计数、全局 `next_offset`、`sealed_through` 以及所有冻结视界
  （`freezes.json`、`end_offset`、段 id 列表）均不变；回放/查询游标语义稳定。
- **陈旧读取防护**：读时损坏隔离带 `expected_sha`；持有旧文件句柄的读者在
  修复原子换入新字节后无法用旧 sha256 把新段再次隔离。
- **重启恢复**：未完成作业在启动时回滚半成品目录后重新入队运行；已记录
  `succeeded` 但崩溃在“换目录与 manifest 提交之间”的作业会在候选字节校验
  通过时补提交、否则回滚，保证已确认数据不丢、段不重。

## 持久化与故障语义

```
$data_dir/
  wal/00000000000000000000.wal     # 追加写，[crc32|len|json] 帧，按封存边界轮转
  segments/seg-00000000000000000000/
    events.log                     # 不可变记录（同 WAL 帧格式）
    index.json                     # 设备 → (seq, offset, event_id, pos, len)
    meta.json                      # offset 区间、计数、sha256、时间窗
  state/manifest.json              # 段目录 + 封存水位（原子替换落盘）
  state/freezes.json               # 冻结视界
  state/repairs.json               # 后台修复作业日志（崩溃恢复/去重依据）
  segments/stage-<job>-<n>/<seg>/  # 修复候选（提交前不触碰 live）
  segments/bak-<job>-<n>/<seg>/    # 原子交换期间的旧段（提交后删除）
```

- **确认即持久**：每批写入先 WAL 追加 + `fsync`，再更新内存态并 ACK；
  段文件 fsync → manifest 原子提交 → WAL 轮转，顺序保证崩溃可恢复。
- **重启恢复**：校验所有封存段（sha256）→ 清理未提交的孤儿段目录 →
  截断 WAL 撕裂尾（torn tail）→ 重放未封存记录 → 重建内存索引。
  WAL 文件中部损坏时隔离该文件、记录 `wal_gaps` 并从下一有效位置继续，
  损失窗口显式可查（`GET /v1/stats`）。
- **冻结可重启**：freeze 元数据落盘，重启后仍可按原视界回放。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `EA_DATA_DIR` | `./data`（容器内 `/data`） | 数据目录 |
| `EA_ADDR` | `0.0.0.0:8080` | 监听地址 |
| `EA_SEGMENT_MAX_RECORDS` | `1000` | 段封存阈值（条数，批内超限自动切分） |
| `EA_SEGMENT_MAX_AGE_SEC` | `300` | 段封存阈值（开放时长，秒） |
| `EA_LATE_THRESHOLD_SEC` | `900` | 迟到判定阈值（秒） |
| `EA_WAL_RETAIN_SEGMENTS` | `8` | 保留多少个已封存段的 WAL 用于重建 |
| `EA_FSYNC` | `1` | 置 `0` 仅用于基准测试（丢失安全性） |
| `EA_MAX_BATCH` | `1000` | 单批最大事件数 |
| `EA_REPAIR_WORKERS` | `2` | 后台修复作业并发工作线程数 |
| `EA_REPAIR_MAX_ATTEMPTS` | `5` | 单个修复遇版本冲突/瞬时 I/O 的最大尝试次数 |
| `EA_REPAIR_RETRY_BACKOFF_SEC` | `0.1` | 重试退避基数（×尝试次数） |
| `EA_REPAIR_HISTORY` | `100` | 作业日志保留的终态作业条数（活动作业不裁剪） |

## 设计取舍与限制

- 单节点；全局锁只保护内存态与 WAL 追加/提交点（均为短临界区），重型读/修复
  I/O 在锁外进行，后台修复作业因此不阻塞前台摄取、检索与快照；吞吐瓶颈在
  fsync 频率，按批聚合。
- 设备索引与去重表在内存中重建（启动重放段索引 + WAL 尾）；事件量级
  超出内存时需外置索引（当前架构可平滑替换 `DeviceState` 的存储）。
- 重复判定依赖 `event_id` 全量驻留内存；`seq` 冲突只打标不拒绝（保留现场）。
- 冻结会触发一次段封存，频繁冻结会产生较多小段。
- 时间戳一律规范化为 UTC ISO-8601；设备时钟仅用于分类，不参与排序。

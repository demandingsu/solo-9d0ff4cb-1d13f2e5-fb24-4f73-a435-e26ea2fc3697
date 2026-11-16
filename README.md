# Battery Cell Cycle Capacity API

分批接收同一电芯的电流采样（UTC 秒 + 电流 A），识别完整充放电循环并按区间
前值积分计算充/放电容量（Ah），通过 FastAPI + SQLite 提供上传与查询接口。

## 业务规则

1. 电流为 0 的采样是**段界**。
2. 只有连续序列 `零 → 正电流 → 零 → 负电流 → 零` 构成一个循环；末尾未闭合
   的序列不计入。
3. 相邻采样时间差 **> 60 秒**即断档：当前序列截断，且该长区间本身不参与积分
   （恰好 60 秒不断档）。
4. 容量按**区间前值积分**（左矩形法）：`Ah += |I_prev| * (t - t_prev) / 3600`，
   充电、放电分别累计。
5. 同电芯同秒同值重复上传**幂等**（不重复插入）；同秒异值则**整批拒收**
   （HTTP 409，库内数据不变）。
6. 迟到补点与历史采样合并后**全量重算**，循环可能被改写、编号重新排列；
   每次成功导入生成一个新的报告版本，历史版本保留。
7. **每次成功上传都分配批次号**（`batch_id`），包括全部为重复采样的批次。
   上传响应携带该号，可据此撤回该批对寿命分析的影响。
8. **按批次撤回**：批次覆盖的采样点，若仍有其他未撤回批次作为依据，则保留；
   旧库中未关联批次的采样（`origin_batch_id IS NULL`）同样保留。只有当某采样
   的**最后一份依据**被撤回时才删除该点，随后重算循环并重新编号，写入新的报告
   版本。重复撤回同一批次是幂等空操作，不改变报告；未知批次返回 404 且不改
   动任何业务状态。
9. 导入或计算失败时事务回滚，旧采样与旧报告保持不变，并在 `upload_log` 留痕。
   撤回失败（含重算异常）同样整事务回滚：批次状态、采样与报告保持原样。
10. 并发：上传与撤回都用 `BEGIN IMMEDIATE` 串行化；查询在单事务快照内读取
    同一版本的版本号、循环明细与总数，保证三者属于同一次已提交状态
    （SQLite WAL）。

## 接口（启动后见 `/docs`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/cells/{cell_id}/samples` | multipart 上传 CSV（字段名 `file`），返回 `batch_id` |
| POST | `/batches/{batch_id}/revoke` | 按批次号撤回；共享/旧采样保留，必要时重算报告 |
| DELETE | `/batches/{batch_id}` | 同上（DELETE 别名） |
| GET  | `/batches/{batch_id}` | 批次元数据与采样依据计数 |
| GET  | `/cells/{cell_id}/batches` | 该电芯的全部批次 |
| GET  | `/cells/{cell_id}/report` | 最新已提交报告：版本、总数、循环明细 |
| GET  | `/cells` | 全部电芯及最新版本概览 |
| GET  | `/health` | 存活探针 |

CSV 必须含表头，列名大小写/空格不敏感，时间列支持
`timestamp/ts/timestamp_utc/timestamp_seconds/utc_seconds/time_s`，
电流列支持 `current/current_a/i/amps` 等。示例见 `examples/`。

```bash
curl -X POST http://localhost:8000/cells/CELL-001/samples \
  -F "file=@examples/sample_one_cycle.csv"
curl http://localhost:8000/cells/CELL-001/report
curl -X POST http://localhost:8000/batches/<batch_id>/revoke
```

## 运行

### Docker Compose（推荐）

```bash
docker compose up --build
# API:   http://localhost:8000
# Docs:  http://localhost:8000/docs
```

SQLite 数据保存在命名卷 `battery-data`（容器内 `/data/battery.db`）。

### 本地运行

```bash
pip install -r requirements.txt
BATTERY_DB_PATH=./data/battery.db uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 测试

```bash
python -m unittest discover -s tests -v       # 算法单测 + HTTP 集成测试
python tests/live_smoke.py                     # 对已运行服务的端到端冒烟
```

覆盖场景：基本循环与前值积分、断档（61s 截断 / 60s 保留）、无零符号直翻、
未闭合不计、补点改写与重新编号、幂等重投、异值整批拒收、失败回滚、
8 写 8 读并发一致性、同秒异值并发竞争，以及批次号返回、共享采样撤回保留、
最后依据撤回后删点重算、重复撤回不变报告、未知批次 404、旧库迁移保留、
撤回中途异常回滚、上传/撤回/查询交错一致性。

## 目录结构

```
app/
  algorithm.py    # 分词 + 状态机循环识别、Ah 积分
  csv_import.py   # CSV 表头解析与校验
  db.py           # SQLite 事务、批次依据、幂等/冲突、版本化报告、撤回
  main.py         # FastAPI 路由
tests/            # 单元、集成、撤回、实时冒烟测试
examples/         # 示例 CSV
Dockerfile, docker-compose.yml
```

## 设计说明与限制

- 电流以**微安整数**存储（`current_ma` 列，单位 µA），使幂等比较为精确整数
  比较，避免浮点文本往返误差；低于 1 µA 的差异会被取整。
- 报告只增不删：每次导入（含纯幂等重投）以及**确实删除了采样**的撤回都追加
  一个 `report_versions` 行，便于追溯；查询接口只返回最新版本。重复撤回不追加
  版本。
- `samples.origin_batch_id` 记录首次插入该点的批次；`sample_batches` 记录
  “批次 → 提交过的点”的完整依据关系（含重复点）。撤回以 `sample_batches`
  为依据判断是否仍有未撤回批次支撑。老库启动时自动 `ALTER TABLE` 增加
  `origin_batch_id`，既有行保持 `NULL`，撤回永不删除它们。
- 单文件 SQLite + WAL 适合本任务量级；写入在进程级连接上串行提交。

# 药检数据采集服务（SFTP → 原始库 → 远端归档）

外包实验室把检测 CSV 通过 SFTP 投递到收件目录；本服务后台扫描批次目录，
校验通过后**整批记录与收件回执一起入库**，再把**远端整目录移动到归档区**。
坏批不入库、不归档并给出原因；归档失败保留已入库结果并自动重试；
进程中断重启自动接续；并发触发不重复入库、不漏批、不提前归档。

## 技术栈

- **FastAPI** + Uvicorn（单进程，内置后台扫描线程）
- **SQLite**（WAL 模式，事务 + 唯一约束保证幂等）
- **paramiko** 真实 SFTP 客户端（连接重试、断线重连、原子 rename 归档）
- **Docker Compose**：API 服务 + 本地 SFTP 源（atmoz/sftp）+ 示例批次

## 目录结构

```
app/
  config.py        环境变量配置
  database.py      SQLite 表结构、批次/记录/回执/开关数据访问
  validation.py    results.csv 校验（非空、列数、批内样品号+项目唯一）
  sftp_client.py   paramiko SFTP 客户端（含本地文件系统测试实现）
  worker.py        后台采集线程：扫描→校验→事务入库→归档→重试/接续
  schemas.py       API 响应模型
  main.py          FastAPI 路由（/docs 交互式文档）
sftp/
  seed/            示例批次（2 好批 + 2 坏批 + 1 未就绪批次）
  seed-on-start.sh SFTP 容器首次启动播种脚本
tests/
  test_e2e.py      端到端：坏批/开关/崩溃接续/并发幂等（本地 FS 模拟 SFTP）
  test_sftp_real.py 真实 paramiko 协议联调（进程内 SFTP 服务端）
  local_server.py  无 Docker 时的本地 HTTP 冒烟启动器
```

## 一键启动（Docker Compose）

```bash
docker compose up --build
```

- API 与交互式文档：http://localhost:8000/docs
- SFTP：`localhost:2222`，用户 `lab` / 密码 `labpass`，收件目录 `/inbound`，归档区 `/archive`
- SQLite 数据持久化在命名卷 `api-data`，SFTP 文件在 `sftp-data`

启动后约 10 秒内后台线程会自动扫描；也可随时调用 `POST /collect` 立即触发。

示例批次：

| 目录 | 内容 | 预期结果 |
| --- | --- | --- |
| `BATCH-2026-1101-A` | 6 条合格记录 | 入库 + 回执 + 归档 |
| `BATCH-2026-1101-B` | 4 条合格记录 | 入库 + 回执 + 归档 |
| `BATCH-2026-1102-BAD-EMPTY` | 空结果/空项目/列数不足 | REJECTED，不入库不归档 |
| `BATCH-2026-1102-BAD-DUP` | 批内样品+项目重复 | REJECTED，不入库不归档 |
| `BATCH-2026-1103-WAITING` | 无 READY | 不被扫描（READY 门控） |

## 在 /docs 里演示完整流程

### A. 正常采集与查询

1. `POST /collect` —— 立即触发一轮扫描（响应里带本轮摘要）
2. `GET /batches` —— 查看批次状态：好批 `ARCHIVED`，坏批 `REJECTED`
3. `GET /batches/{batch_dir}` —— 单个批次详情（坏批的 `error_reasons` 列出每行原因）
4. `GET /batches/{batch_dir}/receipt` —— 收件回执（回执号、记录数、CSV SHA-256）
5. `GET /results?batch_dir=...&sample_no=...&test_item=...` —— 原始记录过滤查询
6. `GET /remote/overview` —— 查看 SFTP 收件箱/归档区当前内容（好批已移走，坏批仍留收件箱）

### B. 故障恢复演练（归档区暂时不可用）

1. `PUT /admin/archive-gate?archive_refused=true` —— 打开“拒绝归档”演示开关
2. 往 SFTP 收件箱投一个新批次（或等待新批次），再 `POST /collect`
3. `GET /batches/{批次}`：状态为 `INGESTED`，`archive_attempts` 递增，
   `error_reasons` 提示将自动重试——**记录已安全入库、远端目录保持原位**
4. 多次 `POST /collect`：确认记录不重复、批次不重复登记
5. `PUT /admin/archive-gate?archive_refused=false` —— 恢复
6. `POST /collect`：待归档批次自动补归档，状态变 `ARCHIVED`，远端目录移入 `/archive`

### C. 崩溃接续（重启验证）

- 场景一：入库后、移动目录前中断 → 重启后扫描发现批次 `INGESTED` 有回执，自动补归档
- 场景二：移动成功、状态更新前中断 → 重启后 `move` 检测归档目标已存在（幂等），对账标记 `ARCHIVED`
- 验证方式：`docker compose restart api`，然后 `POST /collect` 与 `GET /batches` 检查无重复无遗漏

## 关键设计：为什么不会重复入库 / 漏批 / 提前归档

1. **READY 门控**：无 `READY` 的目录视为投递中，整轮跳过；READY 出现后内容冻结。
2. **唯一约束去重**：`batches.batch_dir` 全局唯一；并发或重启后重复登记直接冲突跳过。
3. **单事务整批入库**：记录 + 回执 + 批次状态在一个 `BEGIN IMMEDIATE` 事务内提交，
   崩溃不留半成品；`results(batch_id, sample_no, test_item)` 唯一约束兜底。
4. **先入库后归档**：归档只在事务提交成功后执行，杜绝“已归档但库里没数据”。
5. **归档幂等**：SFTP 同文件系统 `rename` 为原子操作；目标已存在时不覆盖、视为成功，
   因此“移动后崩溃”重启对账不会重复移动或丢数据。
6. **归档失败可重试**：失败仅累加 `archive_attempts` 与原因，批次保持 `INGESTED`，
   每轮扫描开头优先重试所有待归档批次。
7. **扫描串行化**：进程内扫描锁 + 单 Uvicorn worker，并发 `POST /collect` 合并为串行轮次。
8. **坏批隔离**：只写批次状态行（REJECTED + 中文原因），不写 results/receipt，不动远端目录。
9. **重投保护**：已归档批次的同名目录再次出现违反全局唯一约定时，保留现场并在扫描摘要报错，
   不删除、不覆盖、不重复入库。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/collect` | 触发采集扫描（同步等待本轮完成，返回摘要） |
| GET | `/collect/status` | 扫描间隔、最近摘要、线程存活状态 |
| GET | `/batches` | 批次列表，可按 `status` 过滤、分页 |
| GET | `/batches/{batch_dir}` | 批次详情（含坏批原因、归档尝试次数） |
| GET | `/batches/{batch_dir}/receipt` | 收件回执 |
| GET | `/results` | 检测记录查询（批次/样品号/项目过滤、分页） |
| GET | `/admin/archive-gate` | 查看拒绝归档开关 |
| PUT | `/admin/archive-gate?archive_refused=` | 设置拒绝归档开关 |
| GET | `/remote/overview` | SFTP 收件箱与归档区内容 |
| GET | `/health` | 健康检查 |

## 本地测试（无需 Docker）

```bash
python3 -m venv --without-pip .venv          # 系统无 pip 时
.venv/bin/python get-pip.py                  # 引导 pip（如有 pip 可跳过）
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -I tests/test_e2e.py        # 43 项端到端断言
.venv/bin/python -I tests/test_sftp_real.py  # 真实 SFTP 协议联调
```

CSV 约定：首行表头固定为 `sample_no,test_item,result_value`；三列均非空；
批内 `(sample_no, test_item)` 组合唯一。

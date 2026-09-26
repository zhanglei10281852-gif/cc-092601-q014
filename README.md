# 科学计算任务运营服务

这是一个面向科研平台、实验室和计算中心的 Python 后端，使用 FastAPI 与 SQLite 管理参数模板、计算任务提交、优先级排队、工作者领取、取消、失败重试、租约恢复、用户配额、结果版本和管理员人工干预记录。服务同时保留用户、角色、会话和审计等基础能力，所有运行数据都在单个本地数据库文件中，不需要另行部署数据库、缓存、消息队列或浏览器界面。

## 已有能力

- 参数模板：保存参数类型、必填项、数值范围、默认值、最大运行时间和最大尝试次数。
- 任务提交：根据模板校验参数，使用用户与幂等键避免重复创建，并保存项目、提交人和输入摘要。
- 排队领取：按优先级和进入队列的顺序分配任务，工作者可声明算法能力并获得有期限的租约。
- 执行回执：工作者可以续租、提交结果或报告失败；可重试错误使用确定的退避时间重新排队。
- 失败恢复：租约过期后可由恢复入口将任务重新排队，达到最大尝试次数的任务转为失败。
- 配额控制：可保存用户、角色或项目的排队数、运行数和每日提交上限；当前提交路径执行用户配额。
- 结果版本：每次成功回执保存不可变结果、指标摘要和内容摘要，任务指向当前结果版本。
- 人工干预：取消、人工重试、优先级调整和批量操作均保留操作者、原因、前后状态和批次标识。
- 高风险审批：超过阈值的优先级提升、批量重试、强制终止和结果撤回使用可配置审批策略；申请带任务版本快照，申请人与复核人必须分离，批准/驳回/过期/撤销状态完整，等待期任务变化时拒绝套用旧决定；申请、决定、实际干预和最终任务状态串成审计链。
- 登录与角色：基础管理模块提供管理员初始化、用户、角色、会话和细粒度权限。

## 运行环境

- Python 3.11
- SQLite 3，由 Python 标准库提供
- Linux、macOS 或 Windows

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/compute-operations.db`。可以复制 `.env.example` 并通过 `TOWNSHIP_DATABASE_PATH` 指定其他本地路径。

## 数据库初始化与检查

```bash
python -m app.cli init-db
python -m app.cli check-db
```

## 启动 API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

计算任务摘要位于 `/api/compute/summary`，模板、配额、提交、领取、回执和人工操作接口统一使用 `/api/compute` 前缀。

## 高风险干预审批

以下干预由 `compute_approval_policies` 中的策略控制，可通过接口调整阈值、有效期或完全关闭：

| 动作 | 触发条件（默认） |
| --- | --- |
| `priority_boost` 优先级提升 | 目标优先级减当前值达到 `priority_delta_threshold`（默认 20） |
| `batch_retry` 批量重试 | 重试任务数达到 `batch_size_threshold`（默认 2） |
| `force_terminate` 强制终止 | 默认始终需要审批 |
| `result_withdraw` 结果撤回 | 默认始终需要审批 |

- 策略查询与调整：`GET/PUT /api/compute/approval-policies/{action}`。
- 超阈值操作不会立即执行，而是通过 `POST /api/compute/intervention-requests` 生成带任务版本、状态、结果版本和优先级快照的申请（单任务提权、强制终止、结果撤回、批量重试也会自动转入）。
- 复核接口：`/approve`、`/reject`、`/revoke`。申请人与复核人必须为不同人，否则返回 403；只有申请人可以撤销待决申请。
- 申请状态为 `pending / approved / rejected / expired / revoked`；超过 `ttl_seconds` 未决定的申请在下次访问或租约恢复时自动过期。
- 批准时会重新比对任务当前版本与快照：任一任务发生变化即整体拒绝（409，返回差异清单），需要撤销旧申请并基于新版本重新预演；未变化时在同一事务内执行全部任务并写入干预记录，保证批量原子性。
- 批准后重复的批准/驳回返回原决定；创建申请支持幂等键，重复提交返回原申请。
- 审计链：`GET /api/compute/intervention-requests/{id}` 一次返回申请、版本快照、决定、实际干预（`compute_interventions.request_id` 回链）和最终任务状态；任务详情 `GET /api/compute/task-details/{id}` 也可反向查到关联申请。
- 低风险操作（取消、小幅提权、单条重试等）仍直接执行，不产生申请。

## 测试

```bash
python -m pytest
```

测试覆盖参数规则、幂等提交、配额拒绝、优先级领取、能力匹配、租约续期、失败退避、结果版本、取消、人工重试、批量操作、租约恢复、高风险干预审批工作流（提权阈值、批量重试、强制终止、结果撤回、职责分离、过期、撤销、版本漂移拒绝、重复决定与审计链）以及旧库迁移，并保留身份与既有科学计算模块的回归用例。

## 编译检查

```bash
python -m compileall -q app tests
```

## API 冒烟

```bash
python -m app.cli smoke
python -m app.cli compute-demo
```

`smoke` 在进程内检查根路径和健康接口，`compute-demo` 会创建示例参数模板、提交一个计算任务并让匹配能力的工作者领取，用于快速确认核心运营链路。

## 目录结构

```text
app/
  compute/         计算模板、配额、任务、结果版本和人工干预
  api/             用户、角色、认证、审计和系统管理接口
  core/            时钟、安全、异常和分页能力
  repositories/    通用 SQLite 查询
  seismic/         既有地震计算示例领域
  services/        身份、审计和通用后台任务服务
  cli.py           初始化、检查和冒烟入口
  database.py      SQLite 连接、事务、表结构与基础权限
tests/             核心、计算运营和身份回归测试
tools/             本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL、busy timeout 和同步写入策略。提交、领取、回执和人工干预使用即时事务；任务领取通过条件更新避免同一条排队记录被重复领取。服务保存 UTC 时间字符串，测试可以注入固定时钟验证退避、租约到期和跨日配额。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。

# AgenticDMP · 多租户 CDP（对标 Twilio Segment）

一套本地可跑的客户数据平台（CDP）开发环境：后端是**多租户实时 ID-Mapping + 画像宽表 + 圈人引擎**，前端是**对标 [Twilio Segment](https://www.twilio.com/docs/segment) 的控制台**（连接 → 统一 → 触达 → 协议 → 隐私 → 监控）。

**仓库**：https://github.com/MrRobot2658/agenticdmp

> 设计文档：[模块产品文档](./docs/modules/README.md)（分模块 详细/技术设计 + TODOs）· [docs/design.md](./docs/design.md)（Kafka/Flink/MySQL/Doris 宽表）· [scale-comparison](./docs/scale-comparison.md)（dev→2亿）· [Flink Job 模板](./docs/flink/README.md) · [MCP 调用链路](./docs/MCP调用链路.md) · [前端说明](./frontend/README.md)

---

## 一、架构介绍

目标架构（[CDP 优化方案 V3.0-06](./docs/CDP优化方案V3.0-06.pdf) · 数据链路层级，自上而下）。本地 dev 用 **ID-Mapping 服务 + MySQL** 模拟 Flink/Doris；StreamPark / DolphinScheduler / Doris 为生产目标组件。

```
图 1 · 数据链路层级（接入 → Kafka → Flink → MySQL → Doris）

  ┌─ L1 · 接入层（数据入口）
  │    小程序 / 企微 / 表单 / App / 批量导入 API
  │    统一封装为 UserEvent(tenant_id + channel + link_keys + properties)
  ▼
  ┌─ L2 · Kafka 消息层（事件总线）
  │    tenant-{id}-events（大租户独立 Topic） · shared-tenant-events（微租户共享）
  │    按 tenant_id / channel_id 分区，保证同一用户事件有序
  ▼
  ┌─ L3 · Flink 实时计算层（StreamPark 管控）
  │    Job-1 ID-Mapping(OneID 识别 / merge) → Job-2 画像聚合 → Job-3 宽表打宽
  │    输出 enriched-events，并行写入下游存储
  ▼
  ┌─ L3+ · Redis 热层（旁路，< 5ms 点查）
  │    Flink Job-1 同步写入 channel → one_id 映射，供实时识别 / 热点查询
  ▼
  ┌─ L4 · MySQL 业务冷层（持久化 / 审计）
  │    one_id 发号器 · id_mapping 离线导入 · merge_log 合并审计 · 租户配置
  ▼
  └─ L5 · Doris OLAP 层（画像 / 圈选）
       id_mapping · user_profile · user_wide
       大租户独立库 + BE Tag，小租户共享库 tenant_id 分区

  ── 查询层（只读）── SQL Engine + CDP Agent ← 业务应用 / MA / BI；模板查询 → Doris / Redis
  ── 调度管控层（K8s）── StreamPark：Flink Job 启停扩缩 · DolphinScheduler：离线导入/批标签/运维
```

**组件与端口**

| 组件 | 端口 | 说明 |
|------|------|------|
| Nginx 网关 | 8080 | 首页 + CDP 控制台 + API 路由代理 |
| CDP 控制台（前端） | 8080/console/ | 对标 Segment 的 React SPA（生产）；dev 用 5173 |
| SQL Engine | 8002 | OLAP 查询层（模板 SQL + DSL 圈人 + NL + 只读 MCP，与 Doris 解耦） |
| ID-Mapping | 8001 | 实时合并服务 API（OneID 识别 / merge） |
| MySQL 8 | 3308 | 业务库：id_mapping / user_profile / user_groups / merge_log / object_* |
| Redis 7 | 6381 | OneID 热缓存 |
| Kafka / Kafka UI | 9094 / 8083 | 多租户事件总线 / Topic 可视化 |

**前端技术栈**：React 18 + Vite + TailwindCSS。数据链路：浏览器 `/api/*` → （dev: vite 代理 / 生产: nginx）→ **SQL Engine `:8002`** → **MySQL**，毫秒级实时查询。前端信息架构（IA）完全对标 Segment。

---

## 二、功能介绍

控制台按 Segment 的顶层分区组织。**标注说明**：`真实` = 走 SQL Engine→MySQL 的实时查询；`Mock` = 纯前端演示数据（页面右上有「Mock 数据」角标），后端尚未接入（路线图）。

### 概览 Overview `真实`
首页仪表盘：各对象实时计数（用户/客户/订单/商品/门店/标签/受众），快捷入口到「创建受众」「接入数据源」。

### 连接 Connections（数据进出）

| 功能 | 英文 | 状态 | 说明 |
|------|------|------|------|
| 数据源 | Sources | `真实` | 可视化 ETL：CSV/粘贴 → 字段映射(自动/手动) → 预览 → 导入到任意对象，可建关系。MySQL/Kafka/API 为路线图适配器 |
| 数据源详情 | Source Detail | `Mock` | Write Key、Schema 事件、实时事件流 Debugger |
| 目的地 | Destinations | `Mock` | 广告/MA/分析/Webhook 目录，把数据激活到下游 |
| Reverse ETL | Reverse ETL | `Mock` | 以数仓为源，定时回流到目的地 |
| 数据仓库 | Warehouses | `Mock` | Doris/MySQL/Hive 等仓库连接与同步状态 |
| Functions | Functions | `Mock` | 自定义代码在数据源/目的地侧转换数据 |

### 统一 Unify（OneID 画像）

| 功能 | 英文 | 状态 | 说明 |
|------|------|------|------|
| 用户档案 | Profiles | `真实` | 按标识符/属性检索用户宽表（distinct OneID），支持筛选器全部能力 |
| 档案详情 | Profile Detail | `Mock` | 单用户：身份标识、特征 Traits、事件时间线 |
| 身份解析 | Identity Resolution | `Mock` | channel→one_id 合并规则（唯一性/上限/merge 策略） |
| 计算特征 | Computed Traits | `真实` | 标签体系与覆盖人数（来自 /tags） |
| SQL 特征 | SQL Traits | `Mock` | 在数仓上跑自定义 SQL 生成特征 |
| 预测 | Predictions | `Mock` | 购买倾向/流失/LTV 等预测模型 |
| 档案同步 | Profiles Sync | `Mock` | 把统一档案持续同步回数仓 |
| 关联对象 | Objects | `真实` | 客户/订单/商品/门店主数据浏览（同一筛选器，锁定对象） |

### 触达 Engage（圈人与激活）

| 功能 | 英文 | 状态 | 说明 |
|------|------|------|------|
| 受众 | Audiences | `真实` | 已保存人群包列表（来自 /segments） |
| 创建受众 | Build Audience | `真实` | 统一筛选器：多条件、AND/OR、跨对象链式多跳 + 边条件、自然语言圈人；实时预估人数 + SQL 预览；存为受众 |
| 受众详情 | Audience Detail | `Mock` | 规模趋势、连接的目的地 |
| 旅程 | Journeys | `Mock` | 多步自动化旅程与转化 |
| 群发 | Broadcasts | `Mock` | 一次性 Push/短信/EDM 触达 |

### 协议 Protocols（数据治理）`Mock`
埋点计划 Tracking Plans（事件 Schema 校验）· 违规 Violations（类型/必填校验）· 转换 Transformations（入库前重命名/删除/映射）。

### 隐私 Privacy（合规）`Mock`
数据管控 Data Controls（PII 检测与哈希/阻断）· 同意管理 Consent（分类与厂商映射）· 删除与抑制 Deletion & Suppression（GDPR 请求）。

### 监控 Monitor（可观测）`Mock`
投递概览 Delivery（吞吐/成功率/P95/趋势/数据源健康）· 告警 Alerts · 事件日志 Event Delivery。

### 设置 Settings `Mock`
通用 General（工作区信息）· 权限管理 Access Management（IAM 成员/角色）· API 令牌 · 审计日志 Audit Trail。

### 后端服务能力
- **ID-Mapping `:8001`**：多渠道实时合并，OneID 发号、merge 审计、画像查询。
- **SQL Engine `:8002`**：模板 SQL、对象/关系 DSL 圈人、自然语言查询（DeepSeek 意图识别 → 模板 SQL）、可视化 ETL 导入、只读 MCP。

---

## 三、安装

```bash
# 0. 克隆
git clone https://github.com/MrRobot2658/agenticdmp.git   # 或 SSH: git@github.com:MrRobot2658/agenticdmp.git
cd agenticdmp

# 1. 启动后端全栈（MySQL/Redis/Kafka/SQL Engine/ID-Mapping/Nginx）
docker compose up -d --build
docker compose ps                       # 等待 ~30-60s 全部 healthy

# 2. 灌入演示数据（多渠道合并模拟）
bash scripts/simulate_kafka.sh          # 走完整 Kafka 链路（推荐，含租户 1001/1002）
# 或：bash scripts/simulate_via_api.sh  # 直接调 API，跳过 Kafka

# 3. 启动前端（开发态）
cd frontend
npm install
npm run dev                             # → http://localhost:5173/
```

- **生产前端**：由 nginx 挂在 `http://localhost:8080/console/`（`npm run build` 后）；dev 用 vite `:5173`，`/api/*` 自动代理到 SQL Engine `:8002`。
- **DeepSeek（自然语言查询）**：复制 `.env.example` → `.env`，填 `DEEPSEEK_API_KEY`（已 gitignore）。
- **接真实 Doris**：设 `OLAP_BACKEND=doris OLAP_HOST=doris-fe OLAP_PORT=9030`。
- **按规模模拟集群**：`bash scripts/scale-up.sh {dev|large|xlarge}`（<1万 / 1亿 / 2亿）。

---

## 四、使用规则

**访问入口**

| 用途 | 地址 |
|------|------|
| CDP 控制台（开发态） | http://localhost:5173/ |
| CDP 控制台（生产/网关） | http://localhost:8080/console/ |
| SQL Engine Swagger | http://localhost:8002/docs |
| ID-Mapping Swagger | http://localhost:8001/docs |
| MySQL | `localhost:3308` · db `agenticdmp` · user `agenticdmp` / `agenticdmp123` |

**真实 vs Mock**：带「Mock 数据」角标的页面是前端演示，未接后端、改动不落库。**真实数据页**（数据源导入、用户档案 Profiles、受众 Audiences、计算特征）走 SQL Engine→MySQL，操作即落库。

**多租户**：顶栏 Workspace 下拉切换租户（1001/1002）；所有查询自动按 `tenant_id` 隔离。

**圈人规则**：统一筛选器支持 ① 多条件 + AND/OR ② 跨对象链式多跳关联（如 用户→下单→商品）③ 关系边条件 ④ 自然语言（中文描述 → DSL）。查询前可「预估人数」并预览生成的 SQL，满意后「存为受众」。

**ETL 导入规则**：数据源仅 `csv/inline` 可运行；字段按目标对象类型强转，主键缺失会告警，未知字段直接 400；可在导入时按关系建 `object_relations`。

**常用 API / NL 查询**

```bash
# 模板查询：按 OneID 查画像宽表
curl -X POST http://localhost:8002/query/profile_by_one_id \
  -H "Content-Type: application/json" -d '{"params":{"tenant_id":1001,"one_id":100001}}'

# 自然语言查询（经网关）
curl -X POST http://localhost:8080/nl-query \
  -H "Content-Type: application/json" -d '{"question":"查手机号13800138001的用户画像","tenant_id":1001}'

# 渠道 → OneID 映射 / 合并日志
curl http://localhost:8001/mapping/1001/phone/13900001111
curl http://localhost:8001/merge-log/1001
```

**核心表**：`tenants`（租户配置）· `id_mapping`（渠道ID→OneID）· `user_profile` / `doris_user_wide`（画像/宽表）· `object_*`（客户/订单/商品/门店）· `user_groups`/`segments`（受众）· `merge_log`（合并审计）。

**停止与清理**

```bash
docker compose down        # 停止
docker compose down -v     # 停止并清除数据卷
```

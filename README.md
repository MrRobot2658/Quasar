# Quasar · 多数据源计算存储智能代理

**Quasar** 的产品定位是**多数据源计算存储智能代理——为 Agent 提供数据底座**。把散落在各处的数据（数据库/数仓/数据湖/对象存储/消息/API/文件）**统一接入、自动清洗、灵活转化、高性能计算**，以 AI Agent 为自然语言操作入口。向上对 Agent 暴露**结构化数据接口**（OneID 画像宽表 / 圈人结果 / 聚合指标），让上层 Agent 无需关心数据从哪来、怎么算——只管基于高质量的画像和标签做决策。支持**计算存储分离**（计算集群弹性扩缩，Doris 存算分离）也支持**一体化部署**（单机开箱即用），默认存储引擎为 **Apache Doris**。在此之上，多租户隔离、实时 ID-Mapping / OneID 归一、统一画像宽表、圈人激活、数据治理与隐私合规都是开箱功能。并带一套**对标 [Twilio Segment](https://www.twilio.com/docs/segment) 的控制台**作为应用层（连接 → 用户 → 对象 → 客户 → 触达 → 协议 → 隐私 → 监控 → 知识库 → 应用 → 分析）。

> **本地全栈即跑、登录即用**：控制台带**强制登录门禁**（团队成员即登录账号，挂在 workspace 下）；右上角常驻**智能助手**（DeepSeek 对话 + 桥接 MCP 工具 + 后台任务）；可视化编排走**真实 Apache Airflow** 调度；数据源覆盖 **44 个主流连接器**（数据库/数仓/数据湖/对象存储/消息/查询引擎）；并内置**知识库（云盘式多模态存储）**、**应用市场**、**分析看板（NL 一句话生成图表/看板）**。

> **名字含义**：**Agent** —— DeepSeek 驱动的自然语言圈人/查询与 MCP 工具，让 LLM 安全地操作数据平台（候选 DSL 必须过校验层，绝不直出 SQL）；**DataHub** —— 多源数据接入的归一中枢，把小程序/企微/表单/App/批量导入等多渠道数据，实时归一为 OneID 并打宽成统一画像。

**官网**：[https://quasar-site-gray.vercel.app](https://quasar-site-gray.vercel.app) · **产品介绍（在线文档）**：[quasar-site-gray.vercel.app/product.html](https://quasar-site-gray.vercel.app/product.html)（Quickstart → 各模块 → 语义层 → 场景案例 → FAQ → API，含真实截图） · **仓库**：[github.com/MrRobot2658/Quasar](https://github.com/MrRobot2658/Quasar)

![Quasar 控制台](./docs/site/assets/shots/02-console.png)

> 控制台为**对话形态**：左侧会话 · 中间智能助手（Ask / Agent 双模式）· 右侧「数据底座」实时概览 + 「知识库」LLM 上下文策展面板。

> 设计文档（均在 [`docs/`](./docs/README.md) 下，按一级目录分模块）：1 平台底座 + 9 业务模块 + 3 扩展菜单（[知识库](./docs/10-knowledge.md) / [应用](./docs/11-apps.md) / [分析](./docs/12-analyst.md)），各含 详细/技术设计 + TODOs。实时链路架构/规模 → [00-platform](./docs/00-platform.md)；ID-Mapping 画像伸缩 / MCP 调用链路 → [02-unify](./docs/02-unify.md)。· [OpenAPI](./swagger/) · [前端说明](./frontend/README.md)

---

## 一、架构介绍

目标架构（数据链路层级，自上而下；详见 [00-platform](./docs/00-platform.md)）。本地 dev 用 **ID-Mapping 服务 + MySQL** 模拟 Flink/Doris；StreamPark / DolphinScheduler / Doris 为生产目标组件。

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

  ── 查询层（只读）── SQL Engine + 数据 Agent ← 业务应用 / MA / BI；模板查询 → Doris / Redis
  ── 调度管控层（K8s）── StreamPark：Flink Job 启停扩缩 · DolphinScheduler：离线导入/批标签/运维
                          （dev 用 Apache Airflow 承载「可视化编排 Pipelines」的真实调度）
```

**组件与端口**

| 组件 | 端口 | 说明 |
|------|------|------|
| Nginx 网关 | 8080 | 首页 + Quasar 控制台 + API 路由代理 |
| Quasar 控制台（前端） | 8080/console/ | 对标 Segment 的 React SPA（生产）；dev 用 5173 |
| SQL Engine | 8002 | OLAP 查询层 + Agent（模板 SQL + DSL 圈人 + NL 圈人 + 只读 MCP，与 Doris 解耦） |
| ID-Mapping | 8001 | 实时合并服务 API（OneID 识别 / merge） |
| MySQL 8 | 3308 | 业务库：id_mapping / user_profile / user_groups / merge_log / object_* |
| Redis 7 | 6381 | OneID 热缓存 |
| Kafka / Kafka UI | 9094 / 8083 | 多租户事件总线 / Topic 可视化 |
| Airflow | 8088 | 可视化编排 Pipelines 的真实调度后端（admin/admin），SQL Engine 经其 REST API 触发 DAG |
| 智能助手 assistant | 8004 | 多智能体：路由 → 数据查询(MCP)/分析(建图表看板)/任务/通用 |

**前端技术栈**：React 18 + Vite + TailwindCSS。数据链路：浏览器 `/api/*` → （dev: vite 代理 / 生产: nginx）→ **SQL Engine `:8002`** → **MySQL**，毫秒级实时查询。前端信息架构（IA）完全对标 Segment。

---

## 二、功能介绍

控制台按 Segment 的顶层分区组织。**标注说明**：`真实` = 走 SQL Engine→MySQL 的实时查询；`Mock` = 纯前端演示数据（页面右上有「Mock 数据」角标），后端尚未接入（路线图）。

### 总览看板 Overview `真实`
首页**总览看板**：核心 KPI 卡（用户/客户/线索/订单数、GMV、线索转化率）+ 关键分布图表（各对象量、订单状态、线索阶段、客户行业，**点击可下钻明细**）+ 快捷入口（创建受众 / 接入数据源）。复用「分析」的 KPI 与可下钻图表组件。

### 连接 Connections（数据进出）

| 功能 | 英文 | 状态 | 说明 |
|------|------|------|------|
| 数据源 | Sources | `真实` | 数据源卡片列表；可视化 ETL：CSV/粘贴 → 字段映射(自动/手动) → 预览 → 导入到任意对象，可建关系 |
| 数据源目录 | Source Catalog | `真实` | **44 个主流连接器**平铺成卡片（按 8 类分组：数据库/数仓/数据湖/查询引擎/对象存储/流/文件/API），点卡片填名即建数据源。如 MySQL/PostgreSQL/ClickHouse/MongoDB/Snowflake/BigQuery/Iceberg/Delta/S3/Kafka/Trino… 目录级（连接逻辑为占位适配器） |
| 可视化编排 | Pipelines (Flow) | `真实` | 拖拽节点连线编排 ETL → 保存为管道 |
| 管道 | Pipelines | `真实` | 管道列表（行表）→ 详情；点「执行」经 **Apache Airflow** 真实触发 DAG（动态多任务展开）；详情含拓扑、执行历史、暂停/恢复调度 |
| 数据源详情 | Source Detail | `Mock` | Write Key、Schema 事件、实时事件流 Debugger |
| 目的地 | Destinations | `Mock` | 广告/MA/分析/Webhook 目录，把数据激活到下游 |
| Reverse ETL | Reverse ETL | `真实` | 以数仓为源的任务，run-now 触发 + 运行记录（调度模拟） |
| 数据仓库 | Warehouses | `真实` | 连接器目录连数仓（Doris/Snowflake/Iceberg…），同步状态 |
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

### 知识库 Knowledge Base `真实`（右栏面板 · 卡帕西 LLM 模式）
云盘式**多模态文件存储**：上传文档/图片/音视频/压缩包（按类型自动归类 + 图片缩略图）；虚拟目录、搜索、类型筛选；文件详情可预览/下载；**可关联到对象**（user/account/order…，上传时或详情页增删关联）。文件字节存盘（`kb_data` 卷），元数据/关联落库。

控制台右栏「知识库」按**卡帕西 LLM 知识库管理模式**组织（**上下文 = RAM、文件 = 磁盘**）：**分域文件夹**承载多模态资料，文档为知识主干；每个文件/整域可一键**策展进/出 LLM 上下文**（持久化 `kb_files.in_context`），顶部**上下文预算条**按 `token_estimate` 实时估算工作记忆占用。接口：`GET /api/kb/files`、`POST /api/kb/files/{fid}/context`。

### 应用 Apps `真实`（一级菜单）
**应用市场**：按类别平铺第三方应用，连接/断开状态按租户持久化。默认收录 **CRM**（Salesforce / HubSpot / 销售易）· **广告**（广点通 / 巨量引擎 / 百度营销）· **消息**（短信 / 邮件 / 企业微信 / 钉钉）· **分析**（神策 / GA4）。

### 分析 Analyst `真实`（一级菜单）
- **看板列表**：内置 3 个画像看板（**用户画像 / 客户画像 / 转化率ROI**，含 KPI 卡 + 多图）+ 自定义看板。
- **NL 一句话生成**：描述一句话（如「做一个电商运营看板，看订单和商品」）→ DeepSeek 从**白名单数据源目录**受限选图（LLM 不写 SQL）→ 直接生成看板；进详情可**二次编辑**（改标题 / 加减图表）。同样支持 NL 生成单个图表。
- **图表下钻**：点击任意图表的数据点（柱/饼/线）→ 弹出该点背后的明细记录表。
- 图表库 recharts（柱/线/面/饼），数据源为安全聚合（COUNT / 白名单字段 GROUP BY）。

### 智能助手 Assistant `真实`（右上角常驻 · 多智能体）
控制台右上角的对话助手，采用**多智能体**架构：路由器先判定意图，分派给专职智能体，各有独立系统提示与工具集：
- **数据查询** —— 桥接 **41 个只读 MCP 工具**（查 schema/受众/对象/画像…）。
- **分析** —— 自然语言**创建图表 / 看板**（调 `/analyst/*/nl` 落库，回复带「打开看板」直链）。
- **任务** —— 「发布任务」落到 **reverse-ETL 调度模拟**后台运行。
- **通用** —— 产品介绍/答疑 + **页面导航**（无业务工具）。

**打开页面**：助手默认加载 `docs/page-routes.md`（全部功能页面路由），用户说「打开/前往 X 页面」即调用 `open_page` 跳转。回复带「智能体 · X」标识；建好的图表/看板可一键跳转。服务独立（`:8004`）；DeepSeek 不可用时降级。

**主动式埋点 Copilot** —— 助手不只被动等问：前端埋点采集左侧页面行为，近实时推给助手（`POST /observe`），经启发式门控 + LLM 判断后**主动**弹出建议（如**任意页面长时间停留**会主动问询、空结果时建议放宽条件、报错时帮排查）。两层门控控成本，只产出「建议 + action」不自动写，侧边栏铃铛可免打扰。

### 登录 Auth `真实`（强制门禁）
控制台**强制登录**：团队成员（IAM users）即登录账号，挂在 workspace（租户）下；邮箱+密码登录，登录后 workspace 切到该用户租户，右上角显示用户 + 登出。演示账号 `admin@acme.com` / 密码 `demo123`（详见「使用规则」）。

### 设置 Settings
通用 General（工作区信息，`真实`）· 权限管理 Access Management（IAM 成员/角色/团队/邀请，`真实`）· API 令牌（`真实`）· 审计日志 Audit Trail（`真实`）· MCP 设置（展示智能助手可调用的 MCP 工具清单，`真实`）。

### 后端服务能力
- **ID-Mapping `:8001`**：多渠道实时合并，OneID 发号、merge 审计、画像查询。
- **SQL Engine `:8002`**：模板 SQL、对象/关系 DSL 圈人、自然语言查询（DeepSeek 意图识别 → 模板 SQL）、可视化 ETL 导入、只读 MCP。

---

## 三、安装

```bash
# 0. 克隆
git clone https://github.com/MrRobot2658/Quasar.git   # 或 SSH: git@github.com:MrRobot2658/Quasar.git
cd Quasar

# 1. 启动全栈（含前端构建）
#    MySQL/Redis/Kafka/SQL Engine/ID-Mapping/Nginx/Airflow/智能助手 + 前端生产构建
docker compose up -d --build
docker compose ps                       # 等待全部 healthy（Airflow 首次 db migrate 稍慢）
bash scripts/apply_migrations.sh        # 应用增量迁移（含登录/知识库/应用/分析等表与演示数据）

# 2. 灌入演示数据（多渠道合并模拟）
bash scripts/simulate_kafka.sh          # 走完整 Kafka 链路（推荐，含租户 1001/1002）
# 或：bash scripts/simulate_via_api.sh  # 直接调 API，跳过 Kafka

# 3. 打开控制台并登录
#    http://localhost:8080/console/  →  admin@acme.com / demo123

# （可选）前端开发态热更
cd frontend && npm install && npm run dev   # → http://localhost:5173/
```

- **前端已并入 compose**：`frontend` 服务生产构建 → `frontend_dist` 卷，由 nginx 托管于 `http://localhost:8080/console/`，**无需单独启动**。仅改前端时再用 vite `:5173`（`/api/*` 自动代理到 SQL Engine `:8002`）。
- **DeepSeek（自然语言查询）**：复制 `.env.example` → `.env`，填 `DEEPSEEK_API_KEY`（已 gitignore）。
- **接真实 Doris**：设 `OLAP_BACKEND=doris OLAP_HOST=doris-fe OLAP_PORT=9030`。
- **按规模模拟集群**：`bash scripts/scale-up.sh {dev|large|xlarge}`（<1万 / 1亿 / 2亿）。

---

## 四、使用规则

**访问入口**

| 用途 | 地址 |
|------|------|
| Quasar 控制台（开发态） | http://localhost:5173/ |
| Quasar 控制台（生产/网关） | http://localhost:8080/console/ |
| SQL Engine Swagger | http://localhost:8002/docs |
| ID-Mapping Swagger | http://localhost:8001/docs |
| Airflow UI | http://localhost:8088/ · 账号 `admin` / `admin` |
| MySQL | `localhost:3308` · db `dataagent` · user `dataagent` / `dataagent123` |

**真实 vs Mock**：带「Mock 数据」角标的页面是前端演示，未接后端、改动不落库。**真实数据页**（数据源导入、用户档案 Profiles、受众 Audiences、计算特征）走 SQL Engine→MySQL，操作即落库。

**登录**：控制台强制登录。演示账号（密码均 `demo123`）：`admin@acme.com`、`zhang.tech@acme.com`、`zhao.sales@acme.com`、`sun.mkt@acme.com` 等（属技术部/销售部/市场部）。登录后 workspace 自动切到该用户租户。

**多租户**：顶栏 Workspace 下拉切换租户（1001/1002）；所有查询自动按 `tenant_id` 隔离。

**分析看板（NL 生成）**：「分析 › 看板列表」点「新建看板」→ 一句话描述 → 生成并创建 → 进看板可改标题、加减图表；图表点数据点可下钻看明细。内置三个画像看板开箱即用（数据来自演示数据）。

**知识库**：「知识库」上传任意文件，可在上传/详情里关联到对象记录（如 `account/A3001`）；nginx 已放开 `/api` 上传体积上限至 200MB。

**智能助手（多智能体）**：右上角「智能助手」对话。按意图自动分派：**查数据**（MCP 工具）、**建图表/看板**（如「做一个电商运营看板」→ 自动建好并给跳转链接）、**发布后台任务**、**打开页面**（如「打开知识库」「带我去转化率ROI看板」→ 自动跳转）、**通用答疑**。需在 `.env` 配 `DEEPSEEK_API_KEY`。

**可视化编排 Pipelines（Apache Airflow 调度）**

「连接 › 可视化编排」拖拽节点连线 → **保存为管道**；在「管道 Pipelines」页点**执行**，SQL Engine 通过 Airflow REST API 触发 DAG 真实运行（管道页顶部有 Airflow 连接状态条 + 「打开 Airflow」入口）。

- 调度后端：单容器 Airflow（scheduler + webserver，SQLite + SequentialExecutor），UI `http://localhost:8088`（`admin`/`admin`）。
- 承载 DAG：`airflow/dags/dataagent_pipeline.py` —— 一个**参数化通用 DAG**，所有管道运行共用；触发时把 `tenant_id / pipeline_name / 节点列表` 放进 `dag_run.conf`。DAG 用**动态任务映射**（`.expand`）在运行时按 conf 的节点数**展开成多任务**：`plan → run_node × N → finish`，每个画布节点对应一个 task 实例（无需为每个管道单独建 DAG）。画布连线顺序（edges）暂未体现为任务依赖，mapped 任务并行执行。
- 接口：`POST /api/connections/pipelines/{id}/execute` 触发；`GET /api/connections/scheduler/health` 查连通性。Airflow 不可达时执行会**优雅降级**为本地模拟（不报错）。
- 配置（`docker-compose.yml` 的 sql-engine 环境变量）：`AIRFLOW_API_URL` / `AIRFLOW_USER` / `AIRFLOW_PASSWORD` / `AIRFLOW_DAG_ID` / `AIRFLOW_UI_URL`。
- 改/加 DAG：编辑 `airflow/dags/` 下的 py 文件（已挂载进容器），Airflow scheduler 约 30s 内自动加载。
- 生产目标仍是 DolphinScheduler / StreamPark；Airflow 是 dev 下「可视化编排」的真实调度落地。

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

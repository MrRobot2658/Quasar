# 模块 00 · 平台底座 Platform

> 状态：**外壳真实、租户管理后端已落地、鉴权 Mock** · 对标 Segment 的 App Shell + 工作区 + IAM 壳
>
> 本轮落地：`tenant_config` / `tenant_audit` 表 + `tenants` 表生命周期扩展（均「已建」）；sql-engine 新增 `platform_api.py`（`/platform/*` 路由，已挂载于 `main.py`），实现租户 CRUD + 启停 + 每租户配置读写（dry-run 校验）+ 配置变更审计。全局验收 pytest 411P / 0F / 2S。前端管理 UI 仍待接（见 §4）。

## 1. 概述

平台底座是所有业务模块共用的「框架层」：左侧分区导航、顶栏（搜索 + Workspace 切换 + 头像）、页面骨架（标题/副标题/动作位）、UI 组件库、Mock 套件、API 客户端、主题与多租户上下文。它不承载具体业务，但决定了整套控制台「长得像 Segment、跑得通真实数据」。

## 2. 详细设计（产品）

### 2.1 子功能清单

| 功能 | 英文 | 状态 | 说明 |
|------|------|------|------|
| 分区导航 | Sidebar IA | 真实 | 8 大分区 + 设置，激活分区展开二级；中文主 + 英文术语 |
| 工作区切换 | Workspace | 真实(租户)/Mock(组织) | 顶栏切租户 1001/1002，驱动全站按 `tenant_id` 查询 |
| 租户管理 | Tenant Management | 规划 | 租户列表 + 增删改 + **每租户独立配置**（容量/通道/策略/隐私/集成）；见 §2.5 |
| 全局搜索 | Search | Mock | 顶栏搜索框占位（未接） |
| 账号/头像 | Account | Mock | 头像占位，无登录态 |
| 页面骨架 | Page Shell | 真实 | `Layout(title/subtitle/actions)` 统一页头 |
| 品牌主题 | Theme | 真实 | Segment 绿（`brand-*` 调色板）|
| Mock 标注 | MockTag | 真实 | 未接后端页面统一打角标 |

### 2.2 信息架构与页面

- 顶层分区（`lib/nav.ts` 的 `SECTIONS`）：连接 / 统一 / 对象 / 客户 / 触达 / 协议 / 隐私 / 监控；底部 `FOOTER_SECTION`：设置。
- 单页：**总览看板** Overview（`/`，`pages/Dashboard.tsx`）—— 核心 KPI 卡 + 关键分布图表（可下钻）+ 快捷入口；复用「分析」的 `getKpis` 与 `AnalystChart`（真实数据）。
- 租户管理（§2.5）落在 **设置 → 租户管理**（`/settings/tenants`），属平台级治理，详见 [09-settings](./09-settings.md)。
- 骨架：`components/layout/{Layout,Sidebar,Header}.tsx`。

### 2.3 关键用户流程

1. 进入 → 默认总览看板，看核心 KPI 与关键分布（点图表下钻明细）→ 快捷入口进入对应模块。
2. 顶栏切 Workspace（租户）→ 全站数据随 `tenant_id` 刷新。
3. 左栏点分区 → 展开二级 → 进入功能页。

### 2.4 数据模型

本模块已落地三处 schema 变更（`sql/migrate_modules.sql`，经 `scripts/apply_migrations.sh` 应用，全部幂等）。

#### 2.4.1 `tenants` 表生命周期扩展【已建】

通过幂等存储过程 `_add_col` / `_add_idx` 为既有 `tenants` 表新增列（重复执行安全，不改 PK）：

| 列 | 类型 | 默认 | 说明 |
|----|------|------|------|
| `status` | `ENUM('active','suspended')` | `active` | 租户状态，停用后网关可拦截 |
| `scale_tier` | `VARCHAR(32)` | `dev` | dev/medium/large/xlarge，驱动容量配置 |
| `contact_email` | `VARCHAR(128)` | | 租户联系人 |
| `description` | `VARCHAR(512)` | | 租户描述 |
| `max_events_qps` | `INT` | `10000` | QPS 上限（软限） |
| `region` | `VARCHAR(64)` | | 区域 |
| `plan` | `ENUM('starter','business','enterprise')` | `business` | 套餐 |
| `slug` | `VARCHAR(128)` | | 工作区 URL 标识符 |
| `updated_at` | `DATETIME` | `CURRENT_TIMESTAMP ON UPDATE` | 更新时间 |

索引：`idx_status(status)`、`idx_slug(slug)`。基础 4 字段（`tenant_id` / `tenant_name` / `tier` / `kafka_topic`）沿用既有定义。

#### 2.4.2 `tenant_config`——每租户独立配置存储【已建】

运行时热加载，避免全局 env 污染。一行存一个 `(域, 键)` 的配置值。

| 列 | 类型 | 说明 |
|----|------|------|
| `tenant_id` | `BIGINT NOT NULL` | 租户 |
| `config_domain` | `VARCHAR(32) NOT NULL` | 配置域：基础/数据通道/容量/ID-Mapping/存储/隐私/集成/配额 |
| `config_key` | `VARCHAR(64) NOT NULL` | 如 kafka_topic / scale_tier / confidence_threshold / olap_backend |
| `config_value` | `JSON` | 字段值或嵌套配置对象 |
| `created_at` / `updated_at` | `DATETIME` | `updated_at` 带 `ON UPDATE CURRENT_TIMESTAMP` |
| `updated_by` | `VARCHAR(128)` | 更新者标识（预留鉴权） |

主键 `(tenant_id, config_domain, config_key)`；索引 `idx_domain(tenant_id, config_domain)`。`ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`。

#### 2.4.3 `tenant_audit`——配置变更审计日志【已建】

| 列 | 类型 | 说明 |
|----|------|------|
| `audit_id` | `BIGINT AUTO_INCREMENT PK` | 审计主键 |
| `tenant_id` | `BIGINT NOT NULL` | 租户 |
| `actor` | `VARCHAR(128) NOT NULL` | 操作者邮箱或 user_id（默认 `system`）|
| `action` | `VARCHAR(32) NOT NULL` | create / update / suspend / resume |
| `target` | `VARCHAR(64) NOT NULL` | 变更目标：`tenant` / `status` / 某 `config_domain` |
| `old_value` / `new_value` | `JSON` | 变更前后值（`ensure_ascii=false` 存中文） |
| `reason` | `VARCHAR(256)` | 变更原因/备注 |
| `created_at` | `DATETIME` | 默认 `CURRENT_TIMESTAMP` |

索引 `idx_tenant_time(tenant_id, created_at)`。`ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`。

> 说明：「近 24h 事件量」非新建表，由 `merge_log`（ID 合并操作）按 `tenant_id` + 时间窗实时聚合作真实代理。

- 前端无持久化；`TenantContext` 仅内存态（当前 `tenants` 写死 `[1001,1002]`，待改为从 `GET /platform/tenants` 拉取，见 §4）。

### 2.5 租户管理与每租户配置

平台支持对**多个租户**做集中治理：超级管理员维护租户清单，并为**每个租户单独配置**容量、数据通道、ID-Mapping 策略、隐私合规与集成密钥。租户之间配置互不影响——这是「多租户」从「只切 `tenant_id`」升级为「可独立运营」的关键。

#### 2.5.1 子功能

| 功能 | 说明 |
|------|------|
| 租户列表 | 展示所有租户：名称、tier、状态、scale 档位、事件量；支持搜索/筛选 |
| 新建 / 编辑租户 | 表单维护基础信息 + 各配置域；保存前 dry-run 校验（如 topic 冲突、配额合法性）|
| 启用 / 停用 | `status=active/suspended`；停用后该租户写入与查询被网关拦截 |
| 配置详情 | 单租户的「配置中心」，按配置域分 Tab（见 §2.5.2）|
| 配置变更审计 | 每次改配置落 `merge_log` 风格的审计记录（关联 [09-settings](./09-settings.md) Audit Trail）|

#### 2.5.2 每租户配置域（核心）

每个租户拥有一套独立配置，按域划分：

| 配置域 | 字段示例 | 说明 / 关联模块 |
|--------|----------|----------------|
| 基础 | `tenant_name` / `tier`(premium·standard) / `status`(active·suspended) | 租户身份与生命周期 |
| 数据通道 | `kafka_topic` / `topic_mode`(独占·共享) / 分区数 | 独占 `tenant-{id}-events` 或落到 `shared-tenant-events`（见 1003 示例）|
| 容量 / 伸缩 | `scale_tier`(dev·medium·large·xlarge) / Redis 分片 / Flink 并行度 / OLAP 副本 | 对标 [scale-comparison](../scale-comparison.md)；驱动 `scripts/scale-up.sh` 取向 |
| ID-Mapping 策略 | 合并置信度阈值 / 渠道优先级 / 是否启用算法合并 | 注入 id-mapping 服务的合并逻辑 |
| 存储 / OLAP | `OLAP_BACKEND`(mysql·doris) / 库前缀 / 数据保留期 | 后端可按租户隔离（executor 已解耦）|
| 隐私 / 合规 | 数据保留天数 / 默认同意态 / 删除 SLA | 关联 [07-privacy](./07-privacy.md) |
| 集成 | `AGENT_LLM_ENABLED` / LLM Key / destinations 凭证 | 关联 [01-connections](./01-connections.md)、NL→DSL |
| 配额 | 事件 QPS 上限 / 日导入行数 / segment 数上限 | 软/硬限额，超限告警或拒绝 |

> 设计取舍：基础 4 字段沿用现有 `tenants` 表；其余配置域统一落到新表 `tenant_config(tenant_id, domain, config JSON)` 或 `tenants` 扩展列，避免频繁改表结构。读取时按 `tenant_id` 合并出一份「有效配置」。

#### 2.5.3 关键流程

1. 管理员进入 **设置 → 租户管理** → 看租户列表。
2. 新建租户 → 填基础信息 + 选 scale 档位 → 系统建 topic、初始化 `one_id_sequence`、写 `tenant_config`。
3. 编辑某租户的「ID-Mapping 策略」Tab → 改置信度阈值 → 保存 → 审计留痕 → id-mapping 服务热加载新配置。
4. 停用租户 → 网关对该 `tenant_id` 的请求返回 403。

#### 2.5.4 真实 vs Mock 边界

- 现状（本轮后）：`tenant_config` / `tenant_audit` 表【已建】，后端 `/platform/*` CRUD + 配置读写 + 审计【已落地】；**前端管理 UI 仍未接**，全局 env（`OLAP_*`/`AGENT_LLM_ENABLED`/scale 脚本）尚未真正「下沉」消费配置表。
- 落地目标（剩余）：前端管理 UI；让 id-mapping / executor / scale 脚本按 `tenant_id` 读取有效配置，取代全局 env。

## 2.6 逻辑设计（后端，本轮落地）

实现位于 `services/sql-engine/platform_api.py`：自带 `APIRouter(prefix="/platform")`（变量 `router`）+ 本模块私有 Pydantic 模型（刻意不写进 `schemas.py`），`main.py` 第 49/498 行 `import` 并 `include_router`。`PlatformService` 复用 `MysqlOlapExecutor().config` 取连接，全部参数化 SQL，读写按 `tenant_id` 隔离——不手拼 SQL、不改既有 service。经网关访问前缀为 `/api/platform/*`（nginx 剥 `/api`）。

### 2.6.1 端点列表

| Method | Path | 请求 | 响应 |
|--------|------|------|------|
| GET | `/platform/tenants` | query: `search` / `tier` / `status` / `limit`(1–200,默认50) / `offset` | `{tenants:[{tenant_id,tenant_name,tier,status,scale_tier,contact_email,created_at,updated_at,events_count_24h}], total}` |
| POST | `/platform/tenants` | `TenantCreate{tenant_name, tier="standard", scale_tier="dev", contact_email?, description?, actor?}` | `{tenant_id, tenant_name, status, created_at}` |
| GET | `/platform/tenants/{tenant_id}` | path | 租户详情 + `config_summary{域:键数}`；404 不存在 |
| PUT | `/platform/tenants/{tenant_id}` | `TenantUpdate{tenant_name?, tier?, scale_tier?, contact_email?, description?, actor?}`（部分更新）| `{tenant_id, updated_at}`；404 |
| PATCH | `/platform/tenants/{tenant_id}` | `TenantStatusPatch{status: active\|suspended, reason?, actor?}` | `{tenant_id, status, updated_at}`；400 非法 status / 404 |
| GET | `/platform/tenants/{tenant_id}/config` | query: `domain?` | `{tenant_id, 基础{...}, 数据通道{...}, ...}`（按域合并）；404 |
| PUT | `/platform/tenants/{tenant_id}/config` | `TenantConfigUpdate{domain, updates:{key:value}, reason?, actor?}` | `{tenant_id, domain, updated_keys, updated_at}`；400 非法域/冲突 / 404 |
| GET | `/platform/audit/tenant-config` | query: `tenant_id?` / `actor?` / `action?` / `limit` / `offset` | `{audits:[{audit_id,tenant_id,actor,action,target,old_value,new_value,reason,created_at}], total}` |

配置域常量 `CONFIG_DOMAINS = [基础, 数据通道, 容量, ID-Mapping, 存储, 隐私, 集成, 配额]`。

### 2.6.2 核心算法 / 流程

- **创建租户**：`tenant_id` 非自增，取 `MAX(tenant_id)+1`（≥1001）；自动建 `kafka_topic = tenant-{id}-events`；`INSERT IGNORE` 初始化 `one_id_sequence`(next_id=100000)；写入「数据通道.kafka_topic」「容量.scale_tier」两条初始配置；落 `create` 审计。
- **有效配置合并**（`get_config`）：「基础」域恒从 `tenants` 主表派生（tenant_name/tier/status/scale_tier/contact_email/kafka_topic/description），其余 7 域从 `tenant_config` 按行装配，`config_value` 做 JSON 反序列化；未配置的域返回空对象占位。
- **配置 dry-run 校验**（`update_config`）：写前校验 `domain ∈ CONFIG_DOMAINS`、`updates` 非空；「数据通道.kafka_topic」校验全局唯一（与其它租户冲突则 400）。写用 `INSERT ... ON DUPLICATE KEY UPDATE`（幂等 upsert），并把「容量.scale_tier」「数据通道.kafka_topic」回写同步到 `tenants` 主表，保证两处一致。
- **生命周期**（`set_status`）：status 仅允许 active/suspended，映射为 `resume`/`suspend` 审计动作。
- **审计**：所有写操作（create/update/suspend/resume）统一经 `_audit()` 落 `tenant_audit`，记录 old/new JSON（`ensure_ascii=false` 存中文）。

### 2.6.3 与其他模块依赖

- **依赖**：`executor.MysqlOlapExecutor`（取连接/配置）、既有 `tenants` / `one_id_sequence` / `merge_log` 表。
- **被依赖 / 关联**：[09-settings](./09-settings.md) 的 `settings_api.py` 复用 `tenant_config`（基础域读写共用同一存储）；停用态供网关拦截使用（规划）；配置「下沉」后供 id-mapping / executor / scale 脚本按租户消费（规划）。

## 3. 技术设计

### 3.1 前端

| 关注点 | 实现 |
|--------|------|
| 路由 | `App.tsx`（react-router v6，`BASENAME` dev=`/` / prod=`/console`）|
| 导航数据 | `lib/nav.ts`（`HOME` / `SECTIONS` / `FOOTER_SECTION`，类型 `NavSection`/`NavChild`）|
| 骨架 | `Layout` 包 `Sidebar` + `Header` + `<main>`（`max-w-7xl` 容器）|
| 多租户 | `context/TenantContext.tsx`（`useTenant()` 提供 `tenant/setTenant/tenants`）|
| 主题 | `tailwind.config.js` 的 `brand` 调色板（绿）；组件统一用 `brand-*` |
| UI 库 | `components/ui.tsx`：`Card/Badge/Button/DataTable(含 rowLink)/Modal/TextField/Spinner` |
| Segment 套件 | `components/segment/kit.tsx`：`MockTag/StatCards/StatusPill/Catalog/Timeline/EmptyState/SubTabs/Sparkline` |
| API 客户端 | `api/client.ts`（axios，baseURL `/api`，45s 超时）；类型 `api/types.ts` |

### 3.2 后端

- 网关 **Nginx :8080**：`/console/` 托管前端，`/api/*` 反代到 SQL Engine `:8002`。
- 鉴权：**当前无**。Workspace 切换只是前端切 `tenant_id`，无登录、无权限校验。
- 租户管理（**后端已落地**）：sql-engine `platform_api.py` 暴露 `/platform/tenants` CRUD + `PATCH` 启停 + `/platform/tenants/{id}/config` 读写 + `/platform/audit/tenant-config`（详见 §2.6）。`tenants` 扩展列 + `tenant_config` / `tenant_audit` 表经 `scripts/apply_migrations.sh` 迁移（强制 `utf8mb4`）。写操作的超级管理员鉴权仍待 [09-settings](./09-settings.md) IAM（当前 `actor` 由请求体传入，预留）。

### 3.3 真实 vs Mock 边界

- 真实：导航、租户切换、页面骨架、主题、概览计数；**租户管理后端**（CRUD/启停/每租户配置/审计，§2.6）+ `tenant_config` / `tenant_audit` 表。
- Mock / 规划：顶栏搜索、头像/登录态、组织级 Workspace（多租户已有，但无「组织/成员」概念）、**租户管理前端 UI**（后端已就绪，UI 未接）、**配置「下沉」消费**（配置已可存取，但 id-mapping/executor/scale 尚未按租户读取，仍用全局 env）。

### 3.4 依赖与集成

- 被所有业务模块依赖（页面都用 `Layout` + `ui`/`kit`）。
- 与 [09-settings](./09-settings.md) 的 IAM 强相关（真实鉴权落地在 Settings 模块）。

## 4. TODOs

**P0（让底座可登录、可治理）**
- [ ] [后端] 引入鉴权服务：登录、会话/JWT、`tenant_id` 从令牌解析（取代前端硬切）。
- [ ] [前端] 登录页 + 路由守卫 + 401 拦截（axios 拦截器）。
- [ ] [前端] 顶栏头像接真实用户；Workspace 列表来自后端 `GET /tenants` 而非写死 `[1001,1002]`。
- [x] [后端] 新增 `tenant_config` / `tenant_audit` 表 + `tenants` 扩展列 + 迁移（§2.4）；`/platform/tenants` CRUD 与 `/platform/tenants/{id}/config` 读写 + 审计接口（§2.6）。
- [ ] [后端] 配置「下沉」：`OLAP_*` / `AGENT_LLM_ENABLED` / scale 档位等全局 env 改为按 `tenant_id` 读取 `tenant_config` 有效配置（存储已就绪，消费侧未接）。
- [ ] [前端] 设置 → 租户管理页：租户列表 + 新建/编辑 + 各配置域 Tab + 启停 + 变更审计（接 `/platform/*` 端点）。
- [ ] [后端] 停用态拦截：网关/中间件对 `status=suspended` 租户的请求返回 403。

**P1（体验与健壮性）**
- [ ] [前端] 全局搜索接 `objects/search` 跨对象（Sources/Audiences/Profiles）。
- [ ] [前端] 错误边界 + 统一 Toast；移动端折叠侧栏（当前 `lg:` 才显示）。
- [ ] [前端] 面包屑 + 页面级 loading skeleton。

**P2（打磨）**
- [ ] [前端] 暗色主题；i18n 抽出中英文案。
- [ ] [前端] `DataTable` 升级：分页、排序、列配置、CSV 导出。
- [ ] [前端] 把 `mock/data.ts` 按模块拆分，便于各模块独立维护。

## 智能助手（多智能体）

控制台对话助手，独立服务 `services/assistant`（`:8004`，compose `assistant`），经 nginx `/assistant/*` 暴露；前端 `components/assistant/AssistantWidget.tsx` + `api/assistant.ts`。

- **UI：右侧停靠侧边栏（toggle）**：顶栏「智能助手」按钮开/合一个**整高右侧侧边栏**；打开时**主内容右移让位**（lg+，互不遮挡），移动端全屏覆盖。开合状态由 `context/AssistantContext` 管理并持久化（`localStorage`）；`components/assistant/AssistantShell.tsx` 包裹受保护路由提供「推开」布局并挂载侧边栏。助手「打开页面」时侧边栏保持打开，内容在旁边切换。

- **多智能体架构**：`/chat` 先由**路由器**（DeepSeek 分类，失败降级关键词）判定意图，分派给专职智能体，各有独立系统提示与工具集：
  - **data 数据查询** —— 桥接 CDP **MCP server**（stdio，`services/mcp/server.py`）的 41 个只读 `cdp_*` 工具。
  - **analyst 分析** —— 本地工具 `create_chart` / `create_dashboard`，调 sql-engine `/analyst/charts/nl`、`/analyst/dashboards/nl` 落库（回复带「打开看板」直链）。
  - **task 任务** —— `publish_task`，接 reverse-ETL 调度模拟后台运行。
  - **general 通用** —— 产品答疑 + **页面导航**（无业务工具）。
- **打开页面**：所有智能体都带 `open_page(path)` 工具；启动时从挂载的 `docs/page-routes.md`（compose `./docs/page-routes.md:/app/page-routes.md:ro`）**解析全部功能页面路由**注入系统提示，用户说「打开/前往/带我去 X 页面」时调用 `open_page`，前端 `AssistantWidget` 收到 `navigate` 后跳转并收起面板。导航 vs 新建由路由+提示区分（「打开某看板」走导航，「做一个看板」走 create）。
- 响应含 `agent` / `agent_name`（前端显示「智能体 · X」标识）、`steps`（工具调用轨迹）、`task`、`created`（新建图表/看板 + 跳转路径）、`navigate`（要打开的页面）。
- **聊天记录持久化（按用户）**：`/chat` 带 `user_id`（登录用户）即把每轮「用户消息 + 助手回复」落 `assistant_messages` 表（assistant 服务直连 MySQL）；面板首次打开按 `user_id` 加载历史，头部「清空记录」可删除。用户间隔离。
- 端点：`POST /chat`（含 user_id）、`GET /history` / `DELETE /history`（按用户）、`GET /health`（含 agents 列表）、`GET /agents`、`GET /mcp/tools`（设置 → MCP 设置页展示）、`GET /tasks`。表：`sql/migrate_assistant.sql`。
- 降级：无 `DEEPSEEK_API_KEY` 或出错时返回友好提示，绝不 500。

> 真实 vs Mock：**全真实**（对话、MCP 查询、建图表看板、发任务均落地）。

---

> 以下为整理时并入的平台底座技术文档（原 docs/design.md / scale-comparison.md）。

## 多租户用户画像实时链路设计

> 版本 v1.1 | 场景：微信 / 企业微信 / 表单 → 身份合并 → 行为汇总 → 用户画像 → Doris 宽表

### 1. 方案总览

#### 1.1 核心目标

1. 租户数据物理隔离 + 逻辑隔离
2. 按租户数据量水平伸缩（小租户共享、大租户独占）
3. 租户内多渠道实时 ID 打通（OneID）
4. 用户画像属性 + 行为实时更新，秒级可查

#### 1.2 总体架构

```
┌─────────────────────────────────────────────────────────────┐
│                    StreamPark 管控面                          │
│  id-mapping-1001  │  profile-1001  │  wide-1001  │ shared  │
└────────┬──────────────────┬─────────────────┬──────────────┘
         │                  │                 │
    ┌────▼────┐        ┌────▼────┐       ┌────▼────┐
    │ Kafka   │        │ enriched│       │ Kafka   │
    │ events  │───────▶│ events  │──────▶│ sink    │
    └────┬────┘        └────┬────┘       └────┬────┘
         │                  │                 │
    ┌────▼──────────────────▼─────────────────▼────┐
    │  Flink: ID-Mapping → 画像聚合 → 宽表打宽        │
    └────┬──────────────────┬─────────────────┬────┘
         │                  │                 │
    ┌────▼────┐        ┌────▼────┐       ┌────▼──────────┐
    │  Redis  │        │  MySQL  │       │  Apache Doris │
    │  热层   │        │  业务库  │       │  OLAP + 宽表  │
    └─────────┘        └─────────┘       └───────────────┘
```

#### 1.3 本地开发环境映射

| 生产组件 | 本地实现 | 路径 |
|---------|---------|------|
| Kafka | `agenticdatahub-kafka` | `docker-compose.yml` |
| Flink Job | Python id-mapping 服务 | `services/id-mapping/main.py` |
| Redis | `agenticdatahub-redis` | 端口 6381 |
| MySQL | `agenticdatahub-mysql` | `sql/init.sql` |
| Doris | MySQL 模拟表 `doris_*` | `sql/init.sql` |

Flink 生产 Job 模板见 [`docs/flink/`](./flink/README.md)。

---

### 2. Kafka Topic 设计

#### 2.1 Topic 分级

| 租户类型 | Topic | 分区(生产) | 副本 | Message Key |
|---------|-------|-----------|------|-------------|
| premium 大租户 | `tenant-{id}-events` | 16 | 3 | `tenant_id` 或 `channel_id` |
| standard 小租户 | `tenant-{id}-events` | 2~4 | 2 | `tenant_id` |
| 微租户共享 | `shared-tenant-events` | 8 | 2 | `tenant_id`（body 也带 tenant_id） |

**本地环境：**

| Topic | 分区 | 租户 |
|-------|------|------|
| `tenant-1001-events` | 4 | 1001 品牌A |
| `tenant-1002-events` | 2 | 1002 品牌B |
| `shared-tenant-events` | 4 | 1003+ |

#### 2.2 创建命令

```bash
## 大租户
kafka-topics.sh --create \
  --topic tenant-1001-events \
  --partitions 16 --replication-factor 3 \
  --config retention.ms=604800000 \
  --config compression.type=lz4

## 共享 Topic
kafka-topics.sh --create \
  --topic shared-tenant-events \
  --partitions 8 --replication-factor 2
```

#### 2.3 下游 Topic（Flink 输出）

| Topic | 生产者 | 消费者 | 说明 |
|-------|--------|--------|------|
| `enriched-{tenant_id}-events` | Job-1 ID-Mapping | Job-2 画像 / Job-3 宽表 | 带 one_id 的富化事件 |

#### 2.4 消息 Schema（UserEvent）

```json
{
  "event_id": "evt_1718123456789",
  "tenant_id": 1001,
  "channel_type": "form_id",
  "channel_id": "form_lead_abc123",
  "event_type": "form_submit",
  "event_time": "2026-06-12T10:00:00",
  "link_keys": {
    "wechat_unionid": "wx_union_abc",
    "phone": "13900001111"
  },
  "properties": {
    "form_name": "618大促留资",
    "interest": "智能家居",
    "amount": 0,
    "order_count": 1
  }
}
```

#### 2.5 channel_type（身份识别字段）

| 值 | 渠道 | 说明 |
|----|------|------|
| `wechat_openid` | 微信 | 小程序/H5 openid |
| `wechat_unionid` | 微信 | 跨应用统一身份 |
| `wework_extid` | 企业微信 | 外部联系人 ID |
| `form_id` | 表单 | 留资记录 ID |
| `phone` | 通用 | 强关联键 |
| `email` | 通用 | 邮箱 |
| `device` | 通用 | 设备 ID |

#### 2.6 event_type（行为类型）

| 渠道 | event_type |
|------|-----------|
| 微信 | `page_view` / `login` / `bind_phone` |
| 表单 | `form_submit` / `form_update` |
| 企微 | `add_friend` / `send_material` |

#### 2.7 Producer 规范

```java
// 大租户独立 Topic
producer.send(new ProducerRecord<>(
    "tenant-1001-events",
    String.valueOf(tenantId).getBytes(),
    eventJson
));

// 共享 Topic：key 必须为 tenant_id，保证同租户有序
producer.send(new ProducerRecord<>(
    "shared-tenant-events",
    String.valueOf(tenantId).getBytes(),
    eventJson
));
```

---

### 3. Flink 任务设计

拆为 3 个 Job，由 StreamPark 按租户部署。模板代码见 `docs/flink/`。

#### 3.1 Job-1：实时 ID-Mapping（DataStream）

**职责：** 身份识别字段实时合并，输出带 `one_id` 的富化事件。

```
Kafka(user-events)
  → keyBy(tenant_id + channel_type + channel_id)
  → IdMappingFunction
      ├─ Redis 查 channel → one_id
      ├─ miss → Doris/MySQL id_mapping
      ├─ link_keys 跨渠道关联
      ├─ create / link / merge
      ├─ 写 Redis + Doris id_mapping
      └─ 写 MySQL（可选）
  → Kafka(enriched-events)
```

| 配置项 | 大租户 1001 | 小租户 1002 | 共享 Job |
|--------|------------|-------------|---------|
| 并行度 | 16 | 2~4 | 8 |
| State | RocksDB | RocksDB | RocksDB |
| Checkpoint | 60s | 60s | 60s |

> Job-1 含 merge 状态机，**必须用 DataStream**，不适合纯 Flink SQL。

模板：`docs/flink/src/main/java/com/agenticdatahub/flink/IdMappingJob.java`

#### 3.2 Job-2：用户画像实时聚合（Flink SQL）

**职责：** 属性 merge + 行为 append + 标签计算 → `user_profile`。

```
Kafka(enriched-events)
  → GROUP BY tenant_id, one_id
  → 聚合 properties / tags
  → Doris user_profile UNIQUE KEY Upsert
  → Redis 热点画像缓存（Async Sink）
```

模板：`docs/flink/sql/job-02-profile-aggregation.sql`

#### 3.3 Job-3：Doris 宽表实时打宽（Flink SQL）

**职责：** 多渠道身份列展开 + 画像快照 → `user_wide`。

```
Kafka(enriched-events) 或 Doris id_mapping CDC
  → JOIN user_profile
  → 列展开 wechat_openid / wework_extid / form_id / phone ...
  → Doris user_wide UNIQUE KEY Upsert
```

模板：`docs/flink/sql/job-03-wide-table.sql`

#### 3.4 StreamPark 部署矩阵

| Job 名 | 类型 | Source | Sink |
|--------|------|--------|------|
| `id-mapping-1001` | DataStream | `tenant-1001-events` | Redis + Doris + `enriched-1001-events` |
| `profile-1001` | Flink SQL | `enriched-1001-events` | Doris `user_profile` |
| `wide-1001` | Flink SQL | `enriched-1001-events` | Doris `user_wide` |
| `id-mapping-shared` | DataStream | `shared-tenant-events` | 同上，按 tenant_id 过滤 |

---

### 4. MySQL 表设计（业务冷层）

MySQL 承担：发号器、离线映射导入、画像备份、合并审计。

#### 4.1 tenants

```sql
CREATE TABLE tenants (
    tenant_id       BIGINT PRIMARY KEY,
    tenant_name     VARCHAR(128) NOT NULL,
    tier            ENUM('premium', 'standard') NOT NULL DEFAULT 'standard',
    kafka_topic     VARCHAR(128) NOT NULL,
    doris_db        VARCHAR(64) COMMENT 'premium→tenant_{id}',
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;
```

#### 4.2 id_mapping

```sql
CREATE TABLE id_mapping (
    tenant_id       BIGINT NOT NULL,
    channel_type    VARCHAR(32) NOT NULL
        COMMENT 'wechat_openid/wechat_unionid/wework_extid/form_id/phone/email/device',
    channel_id      VARCHAR(256) NOT NULL,
    one_id          BIGINT NOT NULL,
    confidence      DOUBLE DEFAULT 1.0,
    source          VARCHAR(32) DEFAULT 'realtime'
        COMMENT 'offline/login/link/merge/realtime',
    create_time     DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, channel_type, channel_id),
    INDEX idx_one_id (tenant_id, one_id)
) ENGINE=InnoDB COMMENT='渠道身份 → OneID';
```

#### 4.3 one_id_sequence

```sql
CREATE TABLE one_id_sequence (
    tenant_id       BIGINT PRIMARY KEY,
    next_id         BIGINT NOT NULL DEFAULT 100000
) ENGINE=InnoDB;

-- 原子发号
INSERT INTO one_id_sequence (tenant_id, next_id) VALUES (?, 100000)
ON DUPLICATE KEY UPDATE next_id = LAST_INSERT_ID(next_id + 1);
SELECT LAST_INSERT_ID();
```

#### 4.4 user_profile

```sql
CREATE TABLE user_profile (
    tenant_id       BIGINT NOT NULL,
    user_id         BIGINT NOT NULL COMMENT 'OneID',
    channel_type    VARCHAR(32),
    channel_id      VARCHAR(128),
    tags            JSON,
    properties      JSON COMMENT '属性+行为，含 behaviors[]',
    update_time     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, user_id),
    INDEX idx_update_time (update_time)
) ENGINE=InnoDB COMMENT='用户画像';
```

**properties 示例：**

```json
{
  "nickname": "张三",
  "form_name": "618大促留资",
  "amount": 15000,
  "last_behavior": "send_material",
  "last_channel": "wework_extid",
  "behaviors": [
    {"event_type": "page_view",   "channel_type": "wechat_openid", "at": "2026-06-12T10:00:00"},
    {"event_type": "form_submit", "channel_type": "form_id",       "at": "2026-06-12T10:01:00"},
    {"event_type": "add_friend",  "channel_type": "wework_extid",  "at": "2026-06-12T10:05:00"}
  ]
}
```

#### 4.5 merge_log

```sql
CREATE TABLE merge_log (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id       BIGINT NOT NULL,
    event_id        VARCHAR(64),
    action          VARCHAR(32) NOT NULL COMMENT 'create/link/merge',
    one_id          BIGINT NOT NULL,
    channel_type    VARCHAR(32),
    channel_id      VARCHAR(256),
    linked_one_id   BIGINT,
    detail          JSON,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tenant_time (tenant_id, created_at)
) ENGINE=InnoDB COMMENT='身份合并审计';
```

---

### 5. Doris 表设计

#### 5.1 隔离策略

| 租户类型 | 策略 |
|---------|------|
| premium | 独立库 `tenant_{id}`，BE Resource Tag 物理隔离 |
| standard | 共享库 `tenant_shared`，`tenant_id` 分区逻辑隔离 |

#### 5.2 id_mapping

```sql
CREATE TABLE id_mapping (
    tenant_id       BIGINT       NOT NULL,
    channel_type    VARCHAR(32)  NOT NULL,
    channel_id      VARCHAR(256) NOT NULL,
    one_id          BIGINT       NOT NULL,
    confidence      DOUBLE       DEFAULT 1.0,
    source          VARCHAR(32)  DEFAULT 'realtime',
    create_time     DATETIME,
    update_time     DATETIME
)
UNIQUE KEY(tenant_id, channel_type, channel_id)
DISTRIBUTED BY HASH(tenant_id, channel_type, channel_id) BUCKETS 16
PROPERTIES ("enable_unique_key_merge_on_write" = "true");

ALTER TABLE id_mapping ADD INDEX idx_one_id (one_id) USING INVERTED;
```

#### 5.3 user_profile

**大租户：**

```sql
CREATE TABLE tenant_1001.user_profile (
    user_id         BIGINT       NOT NULL,
    channel_type    VARCHAR(32),
    channel_id      VARCHAR(128),
    tags            BITMAP,
    properties      JSON,
    update_time     DATETIME
)
UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 32;
```

**小租户共享：**

```sql
CREATE TABLE tenant_shared.user_profile (
    tenant_id       BIGINT       NOT NULL,
    user_id         BIGINT       NOT NULL,
    channel_type    VARCHAR(32),
    channel_id      VARCHAR(128),
    tags            BITMAP,
    properties      JSON,
    update_time     DATETIME
)
UNIQUE KEY(tenant_id, user_id)
PARTITION BY LIST(tenant_id) (
    PARTITION p_1002 VALUES IN ("1002"),
    PARTITION p_1003 VALUES IN ("1003")
)
DISTRIBUTED BY HASH(tenant_id, user_id) BUCKETS 16;
```

#### 5.4 user_wide（实时打宽）

```sql
CREATE TABLE tenant_1001.user_wide (
    one_id              BIGINT       NOT NULL,

    -- 身份识别字段（列展开）
    wechat_openid       VARCHAR(256),
    wechat_unionid      VARCHAR(256),
    wework_extid        VARCHAR(256),
    form_id             VARCHAR(256),
    phone               VARCHAR(256),
    email               VARCHAR(256),
    device              VARCHAR(256),

    -- 画像汇总
    channel_count       INT          DEFAULT 0,
    tags                BITMAP,
    properties          JSON,

    last_event_time     DATETIME,
    update_time         DATETIME
)
UNIQUE KEY(one_id)
DISTRIBUTED BY HASH(one_id) BUCKETS 32
PROPERTIES ("enable_unique_key_merge_on_write" = "true");

ALTER TABLE user_wide ADD INDEX idx_phone    (phone)        USING INVERTED;
ALTER TABLE user_wide ADD INDEX idx_form_id  (form_id)      USING INVERTED;
ALTER TABLE user_wide ADD INDEX idx_wework   (wework_extid) USING INVERTED;
```

#### 5.5 典型查询

```sql
-- 表单留资反查全渠道身份
SELECT one_id, wechat_openid, wework_extid, phone, properties
FROM tenant_1001.user_wide WHERE form_id = 'form_lead_abc123';

-- 高价值用户圈选
SELECT one_id, phone, properties
FROM tenant_1001.user_wide
WHERE BITMAP_CONTAINS(tags, BITMAP_FROM_STRING('high_value'));

-- 宽表 + 映射明细 JOIN
SELECT w.*, m.channel_type, m.source
FROM tenant_1001.user_wide w
JOIN id_mapping m ON w.one_id = m.one_id
WHERE m.tenant_id = 1001;
```

---

### 6. Redis 热层设计

```
## 渠道 → OneID
SET channel:{tenant_id}:{channel_type}:{channel_id} {one_id}  EX 2592000

## OneID → 所有渠道（反向查询）
HSET uid:{tenant_id}:{one_id}:channels  wechat_openid  oXxx...
HSET uid:{tenant_id}:{one_id}:channels  form_id        form_lead_abc
EXPIRE uid:{tenant_id}:{one_id}:channels 2592000

## 热点画像（可选）
SET profile:{tenant_id}:{one_id} {json}  EX 3600
```

---

### 7. 三层存储职责

| 存储 | 角色 | 典型查询延迟 |
|------|------|-------------|
| Redis | channel↔one_id、热点画像 | \< 5ms |
| MySQL | 发号、离线导入、审计 | 10~50ms |
| Doris | 全量映射、画像、宽表、圈选 | 点查 \< 5ms，圈选 1~3s |

---

### 8. SQL Engine 查询层（与 Doris 解耦）

```
业务应用 / BI
      │
      ▼
┌─────────────┐     模板名 + 参数      ┌──────────────┐
│ SQL Engine  │ ─────────────────────▶ │ OlapExecutor │
│  :8002      │     拼装 SQL           │  (可切换)     │
└─────────────┘                        └──────┬───────┘
                                              │
                         ┌────────────────────┼────────────────────┐
                         ▼                    ▼                    ▼
                   MySQL 模拟           Doris FE:9030        其他 OLAP
                   (本地开发)            (生产环境)
```

- 查询模板：`services/sql-engine/templates/olap_queries.yaml`
- 切换后端：`OLAP_BACKEND=mysql|doris`
- API：`POST /query/{template_name}` + `{"params": {...}}`

### 9. Docker 规模模拟

```bash
bash scripts/scale-up.sh dev      # <1万
bash scripts/scale-up.sh medium   # 1000万
bash scripts/scale-up.sh large    # 1亿
bash scripts/scale-up.sh xlarge   # 2亿
```

详见 [scale-comparison.md](./scale-comparison.md)。

### 10. 相关文件索引

| 文件 | 说明 |
|------|------|
| `sql/init.sql` | MySQL + Doris 模拟表初始化 |
| `sql/migrate_doris.sql` | Doris 模拟层增量迁移 |
| `services/id-mapping/main.py` | 本地 Flink Job 模拟实现 |
| `tests/test_user_profile_realtime.py` | 用户画像 E2E 测试 |
| `docs/flink/README.md` | Flink Job 模板使用说明 |
| `docs/flink/sql/` | Flink SQL Job 模板 |
| `docs/flink/src/main/java/` | DataStream Job 模板（Maven 标准目录） |

---

文档版本: v1.1 | 多租户用户画像实时链路


---

## Docker 规模扩展对照表

模拟生产服务器拓扑，通过 `bash scripts/scale-up.sh <tier>` 一键切换。

| 配置项 | dev (<1万) | medium (1000万) | large (1亿) | xlarge (2亿) |
|--------|-----------|----------------|------------|-------------|
| **Kafka Broker** | 1 | 2 | 3 | 5 |
| **Topic 分区(premium)** | 4 | 8 | 16 | 32 |
| **Topic 副本** | 1 | 1 | 2 | 2 |
| **Redis 节点** | 1 | 2 | 3 | 6 |
| **Redis 内存/节点** | 256MB | 1GB | 2GB | 4GB |
| **MySQL buffer pool** | 256M | 1G | 4G | 8G |
| **id-mapping 副本** | 1 | 2 | 4 | 8 |
| **sql-engine 副本** | 1 | 1 | 2 | 3 |
| **Doris BE 模拟分片** | 1 | 4 | 8 | 12 |

### 启动命令

```bash
bash scripts/scale-up.sh dev       # 默认开发
bash scripts/scale-up.sh medium    # +kafka-2, redis-2
bash scripts/scale-up.sh large     # +kafka-3, redis-3
bash scripts/scale-up.sh xlarge    # +kafka-4/5, 全量节点
```

### 服务端口

| 服务 | 端口 |
|------|------|
| ID-Mapping | 8001 |
| **SQL Engine** | **8002** |
| Kafka broker-1 | 9094 |
| Kafka broker-2 | 9095 (medium+) |
| Redis-1 | 6381 |
| Redis-2 | 6382 (medium+) |
| MySQL | 3308 |

### SQL Engine 切换真实 Doris

```bash
OLAP_BACKEND=doris \
OLAP_HOST=doris-fe \
OLAP_PORT=9030 \
OLAP_DATABASE=tenant_1001 \
docker compose up -d sql-engine
```

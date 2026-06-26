# 13 · 反馈闭环（Feedback Loop）—— CDP 五大 Skill 的「回传 → 迭代」

> 本文是 5 个 CDP Skill **闭环化**的设计与实现说明。前 12 篇文档描述的是「生产 → 应用」的**正向链路**（接入、打通、画像、打标、圈选、触达、分析）；本篇补的是每个 Skill 缺失的**反向链路**：把下游的**消费 / 触达 / 转化 / 质量**结果**回传**给上游资产，驱动资产**自迭代**。

## 0. 为什么需要这一层

5 个 Skill 各自都已有正向半段，缺的全是那个「→ 回传 →」箭头：

| Skill | 正向（已有） | 回传闭环（本篇补） |
|---|---|---|
| 1 客户 360 | 多对象 + OneID + 宽表画像 | 画像**字段被调用频次/场景**回传 → 补缺字段、淘汰无用字段 |
| 2 标签体系 | 多层级标签树 + 打标 + 圈人 | 标签**被圈选频次 + 人群转化率**回传 → 低效降权/归档、高转化加权 |
| 3 人群圈选 | DSL 校验/预估/保存 + NL 圈人 | 人群**触达后打开率/转化率**回传 → 圈选条件自动建议 |
| 4 数据治理 | PII/同意/抑制（合规） | 质量巡检（空值/重复/格式）→ 分级 → 修复 → **下游质量分**回传 → 护栏迭代 |
| 5 分析洞察 | 语义指标 + 图表/看板 | 异常发现（SKU 断崖/客群漂移）→ 归因 → **决策-结果**回传 → 校正 |

**关键观察**：触达回执原料其实已经在库里（`broadcast_sends` 完整记录 sent→delivered→opened→clicked），埋点原料也在（`user_behavior_events`、`event_delivery_log`）。真正缺的是三样东西，本篇统一补齐：

1. **回写列**：资产表（`segments` / `tag_definitions`）上加反馈列，回传了才有地方落。
2. **消费埋点 + 巡检产物表**：`field_usage`（画像字段消费）、`data_quality_checks`（质量分）、`insight_findings`（异常归因）、`tag_usage_log`（标签圈选频次）。
3. **回写 Job**：一个 `FeedbackService` + `/feedback/*` 端点 + 一个 Airflow DAG（`feedback_loop`）周期性聚合回写。

> 设计原则沿用既有约定：**不手拼 SQL 给 LLM 执行**（走模板/参数绑定）；迁移走 `scripts/apply_migrations.sh`（utf8mb4）；回写是**幂等聚合**（重复跑结果一致）；任何依赖缺失（Airflow 宕机/无触达数据）都**优雅降级**，不阻塞正向链路。

---

## 1. 总体架构

```
                         ┌───────────────── 正向链路（已有） ─────────────────┐
  接入 → OneID 打通 → 画像/标签 → 圈选(segments) → 触达(broadcasts) → 分析(semantic)
                         └──────────────────────┬───────────────────────────┘
                                                │ 产生消费/回执/质量信号
                                                ▼
   ┌──────────────────────── 反馈闭环（本篇新增） ────────────────────────┐
   │  埋点/巡检产物表        FeedbackService          回写列                │
   │  field_usage      ┌──> scan_field_health  ──> field_usage.recommend   │
   │  tag_usage_log    ├──> aggregate_segments  ──> segments.* + tag_defs.* │
   │  data_quality_*   ├──> scan_data_quality   ──> data_quality_checks     │
   │  insight_findings └──> detect_insights     ──> insight_findings        │
   │                        record_decision     ──> insight_findings.status │
   └──────────────────────────────┬──────────────────────────────────────┘
                                   │ 由 Airflow DAG「feedback_loop」周期触发
                                   ▼
                       sql-engine /feedback/* 端点
                                   ▼
                  前端 FeedbackCard / assistant show_feedback
```

- **存储**：MySQL（沿用 `MysqlOlapExecutor`，可经 `OLAP_BACKEND` 切 Doris）。
- **服务**：`services/sql-engine/feedback.py`（逻辑）+ `feedback_api.py`（路由，挂 `/feedback`）。
- **调度**：`airflow/dags/feedback_loop.py`，`schedule="@hourly"`，每个聚合步骤一个 task；Airflow 不可达时端点仍可被手动/前端按需调用。
- **展示**：前端 `FeedbackCard`（一个卡片按 `topic` 渲染四类反馈），assistant 增 `show_feedback` 工具与 `feedback` view。

---

## 2. 数据模型（`sql/migrate_feedback.sql`）

### 2.1 `segments` 增列（Skill 3 回写）

| 列 | 类型 | 含义 |
|---|---|---|
| `last_engage_id` | BIGINT | 最近一次触达 `broadcast_id` |
| `sent_count` `opened_count` `clicked_count` `converted_count` | INT | 触达回执累计 |
| `open_rate` `click_rate` `conversion_rate` | DECIMAL(5,2) | 打开/点击/转化率（%） |
| `quality_score` | DECIMAL(4,3) | 人群质量分 0–1（由转化率归一 + 预估准确度） |
| `feedback_at` | DATETIME | 最近回写时间 |

### 2.2 `tag_definitions` 增列（Skill 2 回写）

| 列 | 类型 | 含义 |
|---|---|---|
| `weight` | DECIMAL(4,3) default 1.0 | 标签权重（高转化加权、低效降权） |
| `select_count` | INT | 被用于圈选的累计次数（来自 `tag_usage_log`） |
| `avg_conversion_rate` | DECIMAL(5,2) | 含该标签人群的平均转化率（%） |
| `coverage` | INT | 当前覆盖用户数 |
| `status` | ENUM('active','archived') default 'active' | 低效自动归档 |
| `feedback_at` | DATETIME | 最近回写时间 |

### 2.3 新表

```sql
-- 标签圈选频次日志（Skill 2）：每次 segment 用到某标签记一行
tag_usage_log(id, tenant_id, tag_code, segment_code, used_at)

-- 画像字段消费埋点 + 健康度（Skill 1）
field_usage(
  id, tenant_id, object_type, field_name,
  usage_count,            -- 被圈选/查询累计次数
  last_used_at,
  fill_rate,              -- 填充率%（巡检算）
  distinct_count,         -- 去重值数
  recommendation,         -- keep / enrich(补缺) / deprecate(淘汰)
  scanned_at
)

-- 数据质量巡检结果 + 质量分（Skill 4）
data_quality_checks(
  id, tenant_id, object_type, field_name,
  check_type,             -- null / duplicate / format
  total_rows, bad_rows,
  score,                  -- 0–1，1=无问题
  severity,               -- high / medium / low
  sample,                 -- 命中样例(JSON)
  auto_fixable,           -- 是否可自动修复
  checked_at
)

-- 分析洞察异常发现 + 归因 + 决策回传（Skill 5）
insight_findings(
  id, tenant_id, finding_type,   -- sku_drop / cohort_drift
  dimension, metric,
  baseline, current, change_pct,
  severity,
  attribution,            -- 归因到的维度(JSON)
  status,                 -- open / acknowledged / acted / dismissed
  decision, decision_result,  -- 业务决策 + 结果回传
  created_at, updated_at
)
```

迁移末尾 seed 少量 `broadcasts` / `broadcast_sends` 演示回执（租户 1001），保证 Skill 3 回写有料可聚合。

---

## 3. 服务层（`feedback.py`）逐 Skill 逻辑

### Skill 1 · 客户 360 —— 字段健康度
- `record_field_access(tenant, object, fields[])`：圈选/查询命中字段时 `usage_count += 1`、刷新 `last_used_at`（埋点入口，DSL compile 与 objects/search 调用）。
- `scan_field_health(tenant, object)`：对该对象每个字段算 `fill_rate`（非空率）、`distinct_count`，结合 `usage_count` 给 `recommendation`：
  - `fill_rate < 30%` 且 `usage_count > 0` → **enrich**（有人用但缺数据，建议补缺）；
  - `usage_count = 0` 且 90 天未用 → **deprecate**（无人问津，建议淘汰）；
  - 否则 **keep**。写 `field_usage`。

### Skill 2 · 标签体系 —— 加权/归档
- 圈选时 `log_tag_usage(tenant, segment_code, tag_codes[])` 写 `tag_usage_log`。
- `aggregate_tag_feedback(tenant)`：
  - `select_count` ← `tag_usage_log` 计数；`coverage` ← `count_by_tag`；
  - `avg_conversion_rate` ← 用到该标签的所有 segment 的 `conversion_rate` 均值；
  - `weight` ← 归一化：`clamp(0.2, 2.0, avg_conversion_rate / 租户均值)`；
  - 连续低效（`select_count>0` 但 `avg_conversion_rate` 处于后 10% 且 `coverage` 极低）→ `status='archived'`。回写 `tag_definitions`。

### Skill 3 · 人群圈选 —— 触达回写 + 条件建议
- `aggregate_segment_feedback(tenant)`：对每个有 `broadcast`（`broadcasts.segment_id`）的 segment：
  - 取最新 broadcast 的 `broadcast_stats` → `sent/opened/clicked`；`converted` 用 clicked 近似（dev sim）；
  - `open_rate/click_rate/conversion_rate`；`quality_score = 0.6*conv_norm + 0.4*estimate_accuracy`；
  - 回写 `segments`，并把转化率喂给 Skill 2 的标签聚合。
- `suggest_segment_conditions(tenant, segment_code)`：规则引擎给建议——
  - 若存在同 base_object、转化率更高且覆盖足够的标签 → 「加上标签 X 预计转化率 +N%」；
  - 若条件里含行为时间窗 → 「把窗口从 30 天收窄到 14 天，预估更精准」；
  - 用 `/dsl/estimate` 给出加条件后的预估人数，**不自动执行**，仅建议。

### Skill 4 · 数据治理 —— 质量巡检 + 质量分 + 护栏
- `scan_data_quality(tenant, object)`：对对象核心字段跑三类检查——
  - **null**：空值率；**duplicate**：唯一键重复（如 phone）；**format**：正则（手机 11 位、email 格式）。
  - 每项算 `score = 1 - bad/total`、定 `severity`，命中样例入 `sample`，写 `data_quality_checks`；高危同时聚合进既有 `violations` 表（复用 06 协议模块）。
- `quality_score(tenant, object)`：对象级综合质量分 = 各检查 score 加权平均（回传给下游消费方）。
- 护栏迭代：可修复项（去空格/大小写规范）标 `auto_fixable=1`，供人工裁定或后续自动修复规则采纳。

### Skill 5 · 分析洞察 —— 异常 + 归因 + 决策回传
- `detect_insights(tenant)`：
  - **SKU 断崖**：按 product 聚合近窗 vs 上一窗 GMV/单量，跌幅超阈值 → `finding_type='sku_drop'`，`attribution` 落到具体 product/category；
  - **客群漂移**：用 `audience_size_snapshot` 或标签 `coverage` 的环比变化检测高价值客群占比异常 → `finding_type='cohort_drift'`。
  - 写 `insight_findings`（status='open'）。
- `record_decision(tenant, finding_id, decision, result)`：业务侧把「决策 + 结果」回传，更新 `status='acted'` 与 `decision_result`，形成对检测阈值的人工校正信号。

---

## 4. API（`/feedback/*`，挂 `feedback_api.py`）

| 方法 | 路径 | 作用 |
|---|---|---|
| POST | `/feedback/field/access` | 记录字段消费埋点（内部调用） |
| POST | `/feedback/field/scan` | 触发字段健康巡检 |
| GET | `/feedback/field/health` | 查字段健康度（含 recommendation） |
| POST | `/feedback/segment/aggregate` | 触达回写聚合 |
| GET | `/feedback/segment/quality` | 查 segment 质量榜 |
| GET | `/feedback/segment/suggest` | 圈选条件建议 |
| POST | `/feedback/tag/aggregate` | 标签加权/归档聚合 |
| GET | `/feedback/tag/health` | 查标签健康（weight/转化/状态） |
| POST | `/feedback/quality/scan` | 数据质量巡检 |
| GET | `/feedback/quality/report` | 质量分报告 |
| POST | `/feedback/insight/detect` | 异常发现 |
| GET | `/feedback/insight/findings` | 查洞察发现 |
| POST | `/feedback/insight/decision` | 决策-结果回传 |

所有 GET 走只读；POST 聚合幂等。MCP 只读工具后续可包 `cdp_feedback_*`（不在本期）。

---

## 5. 调度（`airflow/dags/feedback_loop.py`）

`@hourly` DAG，task 链：
`scan_quality` →（并行）`aggregate_segments` → `aggregate_tags` → `scan_field_health` → `detect_insights`。
每个 task `httpx.post` 对应 `/feedback/*` 端点，遍历活跃租户。Airflow 不可达时，前端「立即刷新」按钮与 assistant 工具仍可手动触发同一端点（与既有 `dataagent_pipeline` 的本地兜底一致）。

---

## 6. 前端与助手

- **FeedbackCard**（`frontend/src/components/chat/cards/FeedbackCard.tsx`）：按 `topic ∈ {segment, tag, quality, insight, field}` 调对应 GET，渲染榜单/质量分/建议。
- **ViewCard** 增 `feedback` 分支；`assistant` 增 `show_feedback` 工具 → emit `{type:'feedback', topic}`。
- 入口话术示例：「哪些标签该归档」「这批人群触达效果」「数据质量怎么样」「最近有什么异常」「哪些画像字段没人用」。

---

## 7. 验收

1. `bash scripts/apply_migrations.sh` 落库，新表/新列存在。
2. `POST /feedback/segment/aggregate` 后 `GET /feedback/segment/quality` 返回非空，`segments.conversion_rate` 被回写。
3. `POST /feedback/tag/aggregate` 后高转化标签 `weight>1`、低效标签 `status=archived`。
4. `POST /feedback/quality/scan` 产出 `data_quality_checks` 且综合质量分可查。
5. `POST /feedback/insight/detect` 产出 `insight_findings`，`/feedback/insight/decision` 可回写决策。
6. `pytest tests/test_feedback.py` 冒烟通过（服务在线时）。
7. 前端 FeedbackCard 四类话术可渲染。

> 落地顺序：先 Skill 3（回写离 broadcast 数据最近，最快闭环），再 Skill 2，随后 Skill 4，最后 Skill 1 / Skill 5。

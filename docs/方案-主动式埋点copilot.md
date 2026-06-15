# 方案:主动式埋点 Copilot(让左侧助手「活」起来)

> 目标:左侧业务页面的用户行为被近实时采集 → 推给 assistant 的「主动 agent」→ agent 判断是否值得主动出手 → 有建议时**自动弹出右侧侧边栏**展示一条具体、可执行的建议。

## ✅ 实现状态(已落地)

端到端已打通并验证(LLM 建议生成、冷却限频、无信号静默、行为落库、中文无乱码)。涉及文件:

| 层 | 文件 | 作用 |
|---|---|---|
| DB | `sql/migrate_behavior.sql` | `user_behavior_events` + `proactive_suggestions` 两表 |
| 后端 | `services/assistant/main.py` | `POST /observe`:落库 + `_detect_signal` 启发式门控 + `_copilot_suggest`(DeepSeek 结构化输出)+ 会话级冷却 `PROACTIVE_COOLDOWN_SEC` + 无 LLM 固定文案降级 |
| 前端 | `frontend/src/lib/tracker.ts` | 埋点采集单例:缓冲 + 触发点(切页/idle/攒够N条/强信号)批量上报 + 订阅式派发建议 + 免打扰 |
| 前端 | `frontend/src/components/assistant/useBehaviorTracker.ts` | 接路由/活动/`data-track` 点击,挂在 `AssistantShell` |
| 前端 | `frontend/src/api/assistant.ts` | `observeBehavior()` + 类型 |
| 前端 | `frontend/src/api/client.ts` | 响应拦截器把 API 4xx/5xx 记为 `error` 行为(401 除外) |
| 前端 | `frontend/src/components/assistant/AssistantWidget.tsx` | 主动建议气泡 + action 按钮(打开页面/预填/忽略)+ 免打扰开关 + 收到建议自动展开侧边栏 |
| 前端 | `frontend/src/components/filter/UnifiedFilter.tsx` | 受众预估/检索 0 结果 → emit `empty_state` |

环境开关:`PROACTIVE_ENABLED`(默认开)、`PROACTIVE_COOLDOWN_SEC`(默认 45s);无 `DEEPSEEK_API_KEY` 时走固定文案降级。

---


已确认的三个设计取向:
- 传输:**触发式批量上报**(前端缓冲埋点,命中触发点时批量 POST `/assistant/observe`)。
- 呈现:**有建议时自动弹出并展开右侧侧边栏**(带免打扰开关 + 限频)。
- 数据:**行为事件落库**(新表 `user_behavior_events`,兼做分析/跨会话记忆)。

---

## 1. 总体链路

```
左侧页面交互
   │  (路由切换 / 点击 / 搜索筛选 / 空结果 / 报错 / 停留idle / 反复横跳)
   ▼
前端 tracker（内存 ring buffer，最近~50条）
   │  触发点命中即 flush 一批
   ▼
POST /assistant/observe { tenant, user, session, page, events[] }
   │
   ├─① 落库 user_behavior_events（best-effort，失败不阻塞）
   ├─② 启发式门控（便宜，先于 LLM）：无信号 → 返回 suggestion:null（不烧 token）
   └─③ 命中信号 → copilot agent（DeepSeek，结构化输出）→ 决定是否建议
   ▼
返回 { suggestion: {title,message,action?} | null }
   │
   ▼
前端：suggestion 非空 → setOpen(true) 自动展开 + 插入「主动建议」气泡 + 角标
        action 按钮：打开页面 / 帮我建分群（转预填 user message 走正常 /chat）/ 忽略
```

两层门控(前端触发点 + 后端启发式)保证 LLM 调用稀疏、成本可控。

---

## 2. 前端埋点 tracker

新增 `frontend/src/lib/tracker.ts` + `frontend/src/context/TrackerContext.tsx`(或一个 `useBehaviorTracker()` hook,挂在 `AssistantShell`)。

### 事件类型
| type | 触发源 | 关键 payload |
|---|---|---|
| `page_view` | `useLocation` 路由变化 | path, name, prev_dwell_ms |
| `click` | 带 `data-track="xxx"` 的关键按钮/卡片(事件委托,低侵入) | track_id |
| `search` / `filter` | 搜索框、统一 filter UI(DSL)变更 | keyword?, field/op 计数 |
| `empty_state` | 页面渲染出「空/无数据/0 结果」 | path, count:0 |
| `error` | axios 拦截器(client.ts / assistant.ts)、estimate/validate 失败 | status, endpoint, code |
| `idle` | 当前页停留超阈值(如 20s)无操作 | path, dwell_ms |
| `repeat` | 短时间反复进出同页 / 反复点同处(困惑信号) | path, count |

### 缓冲与上报触发点(任一命中即 flush)
- 路由切换(离开页面)
- idle 超时(如 20s 无操作)
- 事件累计达 N 条(如 10)
- 强信号事件(`error` / `empty_state` / `repeat`)**立即**触发

### 限频与防打扰
- 两次 observe 最小间隔(如 15s)
- 「免打扰」开关(`localStorage`),被 dismiss 的建议进入冷却
- 每会话/每页最多自动弹 X 次

### 接线方式(尽量低侵入)
- 路由:`AssistantShell` 内 `useBehaviorTracker()` 监听 `location`。
- 报错:`client.ts` / `assistant.ts` 的 axios response 拦截器里记一条 `error`。
- 关键按钮:给现有关键 CTA 加 `data-track` 属性,tracker 用全局事件委托统一捕获,无需逐个改 onClick。
- `empty_state`:在通用列表/空态组件里 emit 一次。

---

## 3. 后端:`/observe` 接口 + copilot agent

`services/assistant/main.py` 新增。

### 接口
```
POST /observe
req: { tenant_id, user_id, session_id, page:{path,name},
       events:[...], since_last_suggest_sec }
res: { suggestion: { title, message, action? , confidence } | null }
```

### 处理流程
1. **落库**:批量写 `user_behavior_events`(异步/best-effort)。
2. **启发式门控**(纯代码,便宜):只有命中「值得提示」的信号才进 LLM,否则直接 `suggestion:null`。
3. **copilot agent**(命中信号才调 DeepSeek):
   - 新 persona,system prompt:*你是 CDP 控制台的主动助手;依据用户最近行为 + 当前页面,判断是否要主动给一条具体可执行建议;不确定就沉默(`should_suggest=false`);建议简短,可附一个 action。*
   - **强制结构化输出**:`{ should_suggest, title, message, action?:{type, path?/payload?}, confidence }`。
   - **默认不开 MCP/data agent**(重)——只用「行为 + 页面」上下文 + 轻量本地工具(`open_page` 等)。需要查数时,action 是「帮你去看 X / 帮你查 X」,由用户点击后才走正常 `/chat` data 流程(**写操作仍走人确认**)。
4. **限频/去重**:同会话冷却期内、与上一条高度相似的建议不再发。

### 启发式信号 → 建议(无 LLM 时直接套固定文案,与 `AGENT_LLM_ENABLED=0` 降级一致)
| 信号 | 典型页面 | 主动建议(示例) |
|---|---|---|
| 连续 ≥2 次 0 结果 | Profiles / Audiences | 「条件太严了?用一句话描述人群,我帮你生成分群 DSL」 |
| estimate/validate 报错 | Audiences 新建 | 「这个筛选有字段/操作符问题,要我帮你改正或换自然语言方式吗?」 |
| 新建分群页久留无保存 | Audiences | 「直接告诉我你想圈的人群,我来生成并预估规模」 |
| 刚 ETL 导入成功 | Objects/ETL | 「数据导好了,下一步要不要建个分群或看 Profiles?」 |
| 反复进出 Dashboards 没建图 | Analyst | 「要我帮你建一个常用看板吗?」 |
| 任意页面长时间停留无操作(idle,默认 20s) | 全部 | 主动问候 + 结合本页用途给出具体帮助方向(LLM 生成) |
| 浏览 Identity/Merge Log | Unify | 「需要我解释这条 OneID 的合并规则吗?」 |
| API 5xx | 任意 | 「服务好像有点问题,要不要换个条件再试?」 |

---

## 4. 数据库(`sql/migrate_behavior.sql`,经 `apply_migrations.sh` 应用,utf8mb4)

```sql
CREATE TABLE user_behavior_events (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  tenant_id   BIGINT NOT NULL,
  user_id     BIGINT NOT NULL,
  session_id  VARCHAR(64) NOT NULL,
  event_type  VARCHAR(32) NOT NULL,
  page_path   VARCHAR(255),
  page_name   VARCHAR(128),
  payload     JSON,
  created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_user (tenant_id, user_id, id),
  INDEX idx_session (session_id)
);

-- 可选:记录发出的建议 + 是否采纳,用于调优限频/信号
CREATE TABLE proactive_suggestions (
  id           BIGINT AUTO_INCREMENT PRIMARY KEY,
  tenant_id    BIGINT NOT NULL,
  user_id      BIGINT NOT NULL,
  session_id   VARCHAR(64) NOT NULL,
  trigger_signal VARCHAR(64),
  title        VARCHAR(255),
  message      TEXT,
  action       JSON,
  shown_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
  dismissed    TINYINT DEFAULT 0,
  accepted     TINYINT DEFAULT 0
);
```

行为 payload **只存元信息**(页面路径、动作类型、计数、错误码),不存敏感字段。所有表带 `tenant_id`,observe 校验 token,多租户隔离。

---

## 5. 配置开关(env)
- `PROACTIVE_ENABLED`(默认开)
- `PROACTIVE_COOLDOWN_SEC`(两次主动建议最小间隔)
- `PROACTIVE_OBSERVE_MIN_INTERVAL_SEC`(observe 调用上限)
- 复用 `AGENT_LLM_ENABLED`:关闭时走纯启发式固定文案,不调 LLM。

---

## 6. 前端呈现(自动弹出)
- `AssistantWidget` 增加「主动建议」消息类型(区别于普通对话气泡):标题 + 正文 + 1~2 个 action 按钮。
- observe 返回非空 → `setOpen(true)` 自动展开侧边栏 + 插入气泡 + 角标提示。
- action:
  - `open_page` → `navigate(path)`
  - 「帮我…」→ 把建议转成预填 user message 触发正常 `/chat`(用户确认后才执行,写操作仍走人确认)
  - 「忽略」→ dismiss + 进入冷却
- 免打扰开关存 localStorage;每会话/每页弹出次数封顶。

---

## 7. 落地顺序(里程碑)
1. **打通管道**:DB migration + `/observe`(仅落库 + 返回 null)。可在日志/表里看到事件。
2. **前端 tracker + 接线**(page_view / error / empty),触发上报。行为可见。
3. **启发式信号 + 固定文案建议**(无 LLM)。端到端能弹建议。
4. **copilot LLM agent + 结构化输出**。智能建议。
5. **前端主动气泡 UI + action 按钮 + 免打扰**。
6. **调优**:用 `proactive_suggestions` 采纳率回调限频/信号参数。

---

## 8. 关键不变量(沿用项目约定)
- LLM 绝不直接 emit SQL;任何取数/建分群仍走 `dsl.py` validate→compile 或 `engine.py` 模板。
- copilot 只产出「建议 + 可选 action」,**不自动执行写操作**;保存分群/发任务仍走人确认端点。
- 迁移一律走 `scripts/apply_migrations.sh`(utf8mb4)。
- 文案/注释中文,与现有风格一致。

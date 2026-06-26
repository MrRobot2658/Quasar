"""AgenticDataHub「智能助手」聊天后端（多智能体）。

架构：路由器（router）先判定意图，分派给专职智能体，各智能体有自己的系统提示与工具集：
  - data    数据查询：桥接 CDP MCP 只读工具（schema/search/estimate/画像/受众…）。
  - analyst 分析：创建图表 / 看板（调 sql-engine 的 NL 分析端点）。
  - task    任务：发布后台任务（reverse-ETL 调度模拟）。
  - general 通用：产品介绍 / 答疑 / 闲聊（无工具）。

设计要点：
  - LLM 只通过工具读「智能实时数据底座」，写操作（建图表/看板/任务）走受控端点。
  - 仅 data 智能体需要开 MCP 会话；其余本地工具直连 sql-engine。
  - 无 DeepSeek Key / 出错时降级，绝不 500，返回友好提示。
"""

import json
import os
import sys
import threading
import time
from typing import Any

import httpx
import pymysql
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# ── 环境变量 ────────────────────────────────────────────────────────────────
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_API_BASE = os.getenv("DEEPSEEK_API_BASE", "https://api.deepseek.com").rstrip("/")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
SQL_ENGINE_URL = os.getenv("SQL_ENGINE_URL", "http://sql-engine:8000").rstrip("/")
# CDP API Key：sql-engine 配置 CDP_API_KEY 时所有请求须带 X-API-Key（直连不经网关，需自行注入）。
CDP_API_KEY = os.getenv("CDP_API_KEY", "").strip()
SQL_HEADERS = {"X-API-Key": CDP_API_KEY} if CDP_API_KEY else {}
MCP_SERVER_PATH = os.getenv("MCP_SERVER_PATH", "/app/mcp/server.py")
MCP_SQL_ENGINE_URL = os.getenv("MCP_SQL_ENGINE_URL", SQL_ENGINE_URL)

MAX_TOOL_ITERS = 6

# 主动式埋点 Copilot 开关
PROACTIVE_ENABLED = os.getenv("PROACTIVE_ENABLED", "1") not in ("0", "false", "False", "")
PROACTIVE_COOLDOWN_SEC = int(os.getenv("PROACTIVE_COOLDOWN_SEC", "45"))
_SUGGEST_COOLDOWN: dict[str, float] = {}  # session_id -> 上次发出建议的时间戳

_TOOL_SCHEMA_CACHE: list[dict] | None = None
_TASK_STORE: list[dict] = []

# ── 聊天记录持久化（按用户）─────────────────────────────────────────────────
MYSQL_CFG = {
    "host": os.getenv("MYSQL_HOST", "mysql"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "dataagent"),
    "password": os.getenv("MYSQL_PASSWORD", "dataagent123"),
    "database": os.getenv("MYSQL_DATABASE", "dataagent"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}


def _db():
    return pymysql.connect(**MYSQL_CFG, autocommit=True)


def _save_messages(tenant_id: int, user_id: int | None, conversation_id: str | None, rows: list[tuple]) -> None:
    """rows: [(role, content, agent), ...]。无 user_id 或出错时静默跳过。"""
    if not user_id or not rows:
        return
    try:
        with _db() as conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO assistant_messages (tenant_id, user_id, conversation_id, role, content, agent) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                [(tenant_id, user_id, conversation_id, r, c, a) for (r, c, a) in rows],
            )
    except Exception:  # noqa: BLE001
        pass


def _load_history(tenant_id: int, user_id: int | None, conversation_id: str | None = None, limit: int = 50) -> list[dict]:
    if not user_id:
        return []
    try:
        with _db() as conn, conn.cursor() as cur:
            if conversation_id:
                cur.execute(
                    "SELECT role, content, agent, created_at FROM assistant_messages "
                    "WHERE tenant_id=%s AND user_id=%s AND conversation_id=%s ORDER BY id DESC LIMIT %s",
                    (tenant_id, user_id, conversation_id, min(max(limit, 1), 200)),
                )
            else:
                cur.execute(
                    "SELECT role, content, agent, created_at FROM assistant_messages "
                    "WHERE tenant_id=%s AND user_id=%s ORDER BY id DESC LIMIT %s",
                    (tenant_id, user_id, min(max(limit, 1), 200)),
                )
            rows = cur.fetchall()[::-1]
        for r in rows:
            r["created_at"] = str(r.get("created_at")) if r.get("created_at") else None
        return rows
    except Exception:  # noqa: BLE001
        return []


def _list_conversations(tenant_id: int, user_id: int | None, limit: int = 50) -> list[dict]:
    """会话列表：按 conversation_id 聚合，标题取该会话最早的用户消息。"""
    if not user_id:
        return []
    try:
        with _db() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT conversation_id, MAX(id) AS last_id, MAX(created_at) AS updated_at, COUNT(*) AS cnt "
                "FROM assistant_messages WHERE tenant_id=%s AND user_id=%s AND conversation_id IS NOT NULL "
                "GROUP BY conversation_id ORDER BY last_id DESC LIMIT %s",
                (tenant_id, user_id, min(max(limit, 1), 100)),
            )
            convs = cur.fetchall()
            out = []
            for c in convs:
                cid = c["conversation_id"]
                cur.execute(
                    "SELECT content FROM assistant_messages WHERE tenant_id=%s AND user_id=%s "
                    "AND conversation_id=%s AND role='user' ORDER BY id ASC LIMIT 1",
                    (tenant_id, user_id, cid),
                )
                first = cur.fetchone()
                title = (first["content"][:30] if first and first.get("content") else "新会话")
                out.append({"conversation_id": cid, "title": title,
                            "updated_at": str(c.get("updated_at")) if c.get("updated_at") else None,
                            "count": c.get("cnt")})
        return out
    except Exception:  # noqa: BLE001
        return []

# ── 智能体定义 ──────────────────────────────────────────────────────────────
AGENT_DEFS: dict[str, dict] = {
    "data":    {"name": "数据查询", "desc": "查询底座数据(用户/线索/客户/订单/标签/画像)、圈人、**保存受众入库**、把知识纳入/移出 LLM 上下文"},
    "analyst": {"name": "分析",     "desc": "创建图表或看板、出指标分布（电商/线索/客户等）"},
    "task":    {"name": "任务",     "desc": "发布/运行后台批处理任务：数据同步、导出、跑批、调度（不含保存受众）"},
    "general": {"name": "通用",     "desc": "产品介绍、使用答疑、打开/前往/跳转到某功能页面（导航）、其它对话"},
}

AGENT_SYSTEM: dict[str, str] = {
    "data": (
        "你是 AgenticDataHub 的「数据查询」智能体。**首选**用 show_profile / show_audience / show_table "
        "把结果作为卡片直接呈现给用户——这些渲染工具会自行取数与预估，"
        "**不要**先用 cdp_nl_segment / cdp_estimate / cdp_schema 反复试探。判断方式："
        "看某个用户→show_profile(one_id)；圈人群/想知道某人群规模→show_audience(query)；"
        "看某类对象的记录列表→show_table(object, query)。"
        "只有当用户问的是必须用文字回答的具体事实（如精确计数、对比、解释字段含义）时，才用 cdp_* 只读工具查询后用文字作答。"
        "【指标/口径】问某业务指标的取值/口径/怎么算（退款率/GMV/客单价/支付率/线索合格率等）→ 用 explain_metric(query)，"
        "回答时给出「取值 + 口径(定义) + 公式」，若有关联知识文档(knowledge)也一并提及。"
        "【实体上下文】用户想要某个客户/用户的「完整情况/背景/上下文」或围绕某实体做分析 → 用 entity_context(object, id) "
        "取「数据+关联对象+关联知识」整包，再据此综合作答。"
        "【归因链/关系链】用户想「走一条归因链 / 顺着关系链分析 / 看 X 关联到什么」→ 用 relation_chain(object, id) "
        "得到跨链图，按 实体→关联实体→… 串成链路叙述，并点出沿途的关联知识文档。"
        "【写操作】用户说「把这批人/这个人群**存为/保存为**受众叫X」→ 调用 save_audience(name=X, query=人群描述)；"
        "用户说「把…**纳入/移出上下文**（知识库）」→ 调用 curate_knowledge(query=关键词, in_context=true/false)。"
        "save_audience 返回 need_clarification 时，把澄清问题转述给用户，不要硬存。"
        "当前 tenant_id 是 {tenant_id}，调用需要 tenant_id 的工具时务必带上。回答简洁，用用户的语言。"
    ),
    "analyst": (
        "你是 AgenticDataHub 的「分析」智能体。用户想要图表/看板/指标时：单个图表用 `create_chart`，"
        "一个含多图的看板用 `create_dashboard`，把用户需求原样作为 question 传给工具。当前 tenant_id 是 {tenant_id}。"
        "建好后简要说明名称即可；图表会直接在对话中以卡片渲染（无独立页面/查看器，结果都在对话里）。回答简洁。"
    ),
    "task": (
        "你是 AgenticDataHub 的「任务」智能体。用户要发布/运行后台任务（数据同步、导出、跑批等）时调用 `publish_task`。"
        "注意：若用户是想「**保存/存为受众**」而非跑批，请改用 `save_audience(name, query)`，不要用 publish_task。"
        "当前 tenant_id 是 {tenant_id}。回答简洁。"
    ),
    "general": (
        "你是 AgenticDataHub 的智能助手（通用）。AgenticDataHub 是智能实时数据底座，含连接/用户/对象/客户/触达/"
        "知识库/应用/分析等。做产品介绍与答疑；当用户想查数据/建图表看板/发任务时，引导其说清需求。回答简洁，用用户的语言。"
    ),
}

# ── 功能页面路由表（默认加载，供「打开页面」导航）─────────────────────────────
import re

PAGE_ROUTES_FILE = os.getenv("PAGE_ROUTES_FILE", "/app/page-routes.md")


def _load_pages() -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    try:
        with open(PAGE_ROUTES_FILE, encoding="utf-8") as f:
            for line in f:
                m = re.match(r"^- `(/[^`]*)` — (.+)$", line.strip())
                if m:
                    out.append((m.group(1), m.group(2).strip()))
    except Exception:  # noqa: BLE001
        pass
    return out


PAGE_CATALOG = _load_pages()
PAGE_INDEX = {p: label for p, label in PAGE_CATALOG}
NAV_SUFFIX = (
    "\n\n【页面导航】当用户想「打开/前往/跳转到」某功能页面时，调用 `open_page(path)`；"
    "path 必须取自下表，勿编造：\n" + "\n".join(f"- {p} — {label}" for p, label in PAGE_CATALOG)
) if PAGE_CATALOG else ""

# chat-native：本产品没有独立页面，所有结果都以卡片形式直接呈现在对话流里。
SHOW_SUFFIX = (
    "\n\n【对话即界面】本产品没有独立页面，所有结果都以卡片形式直接呈现在对话里。当用户想：\n"
    "- 看某个用户的画像 → 调用 show_profile(one_id)\n"
    "- 圈人群 / 估算人群规模 → 调用 show_audience(query)，query 用一句话描述目标人群\n"
    "- 看某类对象的记录列表（用户/线索/客户/订单/商品/门店）→ 调用 show_table(object, query)\n"
    "- 看图表/分布/趋势/占比 → 调用 show_chart(question)\n"
    "调用渲染工具后，用一两句话点评结果，并可顺势追问下一步；需要精确数字或校验时才用 cdp_* 工具。"
)

OPEN_PAGE_TOOL = {"type": "function", "function": {
    "name": "open_page",
    "description": "在控制台打开/跳转到一个功能页面。path 必须是已知页面路由之一（见系统提示的页面表）。",
    "parameters": {"type": "object", "properties": {
        "path": {"type": "string", "description": "页面路由，如 /analyst 或 /knowledge"},
    }, "required": ["path"]}}}

app = FastAPI(title="AgenticDataHub 智能助手（多智能体）")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    tenant_id: int
    messages: list[ChatMessage]
    user_id: int | None = None
    conversation_id: str | None = None
    mode: str = "agent"  # agent=可调用工具/渲染卡片；ask=只回答与解释，不执行操作


# ── MCP 桥接 ────────────────────────────────────────────────────────────────
def _mcp_params() -> StdioServerParameters:
    return StdioServerParameters(
        command=sys.executable, args=[MCP_SERVER_PATH],
        env={**os.environ, "SQL_ENGINE_URL": MCP_SQL_ENGINE_URL, "no_proxy": "*"},
    )


def _mcp_tool_to_function(t: Any) -> dict:
    return {"type": "function", "function": {
        "name": t.name, "description": t.description or "",
        "parameters": t.inputSchema or {"type": "object", "properties": {}}}}


async def _fetch_tool_schemas() -> list[dict]:
    global _TOOL_SCHEMA_CACHE
    async with stdio_client(_mcp_params()) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = (await session.list_tools()).tools
            schemas = [_mcp_tool_to_function(t) for t in tools]
    _TOOL_SCHEMA_CACHE = schemas
    return schemas


def _extract_mcp_result(mcp_res: Any) -> Any:
    content = getattr(mcp_res, "content", None)
    if content:
        text = getattr(content[0], "text", None)
        if text is not None:
            try:
                return json.loads(text)
            except Exception:  # noqa: BLE001
                return text
    return {"ok": True}


# ── 本地工具 ────────────────────────────────────────────────────────────────
PUBLISH_TASK_TOOL = {"type": "function", "function": {
    "name": "publish_task",
    "description": "发布一个后台任务（接入 reverse-ETL 调度模拟），立即返回任务ID，任务在后台运行",
    "parameters": {"type": "object", "properties": {
        "task_name": {"type": "string", "description": "任务名称"},
        "source_object": {"type": "string", "description": "源对象，如 user/lead/account/order", "default": "user"},
    }, "required": ["task_name"]}}}

CREATE_CHART_TOOL = {"type": "function", "function": {
    "name": "create_chart",
    "description": "用自然语言创建并保存一个图表（柱/线/饼/面），返回图表标题。question 传用户对图表的描述。",
    "parameters": {"type": "object", "properties": {
        "question": {"type": "string", "description": "对图表的中文描述，如：按城市看线索分布的柱状图"},
    }, "required": ["question"]}}}

CREATE_DASHBOARD_TOOL = {"type": "function", "function": {
    "name": "create_dashboard",
    "description": "用自然语言创建并保存一个看板（含多张图），返回看板标题与查看路径。question 传用户对看板的描述。",
    "parameters": {"type": "object", "properties": {
        "question": {"type": "string", "description": "对看板的中文描述，如：做一个电商运营看板，看订单和商品"},
    }, "required": ["question"]}}}

# ── 渲染指令工具（chat-native）：调用后在对话里直接长出对应卡片，由前端取数渲染 ──────────
SHOW_PROFILE_TOOL = {"type": "function", "function": {
    "name": "show_profile",
    "description": "在对话里渲染某个用户的「画像360」卡片（身份标识/渠道分布/行为时间线）。",
    "parameters": {"type": "object", "properties": {
        "one_id": {"type": "integer", "description": "用户 OneID，如 100002"},
    }, "required": ["one_id"]}}}

SHOW_AUDIENCE_TOOL = {"type": "function", "function": {
    "name": "show_audience",
    "description": "在对话里渲染「人群预估」卡片：把一句话目标人群翻译成 DSL、预估规模，并提供『保存为人群』按钮。",
    "parameters": {"type": "object", "properties": {
        "query": {"type": "string", "description": "用一句话描述目标人群，如：近30天在抖音点过广告且下过单的高价值用户"},
    }, "required": ["query"]}}}

SHOW_TABLE_TOOL = {"type": "function", "function": {
    "name": "show_table",
    "description": "在对话里渲染某类对象的「记录表格」卡片。query 可选，用一句话描述筛选条件。",
    "parameters": {"type": "object", "properties": {
        "object": {"type": "string", "description": "对象类型",
                   "enum": ["user", "lead", "account", "product", "store", "order"]},
        "query": {"type": "string", "description": "可选：一句话筛选条件，如『北京的线索』；留空则列出该对象记录"},
    }, "required": ["object"]}}}

SHOW_CHART_TOOL = {"type": "function", "function": {
    "name": "show_chart",
    "description": "在对话里渲染一张「图表」卡片（柱/线/饼/面），question 传对图表的中文描述。",
    "parameters": {"type": "object", "properties": {
        "question": {"type": "string", "description": "对图表的描述，如：按渠道看用户分布的柱状图"},
    }, "required": ["question"]}}}

SHOW_TOOLS = [SHOW_PROFILE_TOOL, SHOW_AUDIENCE_TOOL, SHOW_TABLE_TOOL, SHOW_CHART_TOOL]

# ── 写操作工具（高频，安全可回滚）─────────────────────────────────────────────
SAVE_AUDIENCE_TOOL = {"type": "function", "function": {
    "name": "save_audience",
    "description": "把一句话描述的人群保存为受众（先 NL→候选DSL→校验，再落库）。用户说「存为/保存为…受众/人群」时调用。",
    "parameters": {"type": "object", "properties": {
        "name": {"type": "string", "description": "受众名称，如「高价值新客」"},
        "query": {"type": "string", "description": "人群的一句话描述，如「近30天下过单且客单价大于500的用户」"},
    }, "required": ["name", "query"]},
}}
CURATE_KNOWLEDGE_TOOL = {"type": "function", "function": {
    "name": "curate_knowledge",
    "description": "把知识库里的资料纳入/移出 LLM 上下文（策展）。用户说「把…纳入上下文/移出上下文」时调用。",
    "parameters": {"type": "object", "properties": {
        "query": {"type": "string", "description": "要策展的文件关键词或文件名，如「质检报告」「指标口径」"},
        "in_context": {"type": "boolean", "description": "true=纳入上下文，false=移出。默认 true"},
    }, "required": ["query"]},
}}
EXPLAIN_METRIC_TOOL = {"type": "function", "function": {
    "name": "explain_metric",
    "description": "查询某业务指标的实时取值 + 口径(定义) + 计算公式 + 关联知识库文档（语义层）。用户问「X 是多少 / X 的口径 / X 怎么算」(如 退款率 / GMV / 客单价 / 支付率 / 线索合格率) 时调用。",
    "parameters": {"type": "object", "properties": {
        "query": {"type": "string", "description": "指标名或中文，如 退款率 / gmv / 客单价"},
    }, "required": ["query"]},
}}
ENTITY_CONTEXT_TOOL = {"type": "function", "function": {
    "name": "entity_context",
    "description": "取某个业务实体的完整上下文包：结构化数据 + 关联对象 + 关联知识库文档（Entity Hub）。用户问「把客户/用户 X 的完整情况/上下文/背景给我」「围绕 X 做分析」时调用。",
    "parameters": {"type": "object", "properties": {
        "object": {"type": "string", "description": "对象类型：user/account/order/lead/product/store"},
        "id": {"type": "string", "description": "对象主键值，如 A3001 或 OneID"},
    }, "required": ["object", "id"]},
}}
RELATION_CHAIN_TOOL = {"type": "function", "function": {
    "name": "relation_chain",
    "description": "从一个实体出发沿关系图多跳遍历，得到「实体→关联实体→…」的跨链图，每节点带关联知识库文档（归因链/关系跨链）。用户想「走一条归因链 / 看 X 关联了什么 / 顺着关系链分析」时调用。",
    "parameters": {"type": "object", "properties": {
        "object": {"type": "string", "description": "起点对象类型：user/account/order/lead/product/store"},
        "id": {"type": "string", "description": "起点主键值，如 A3001"},
        "max_hops": {"type": "integer", "description": "最大跳数，默认 3"},
    }, "required": ["object", "id"]},
}}
WRITE_TOOLS = [SAVE_AUDIENCE_TOOL, CURATE_KNOWLEDGE_TOOL, EXPLAIN_METRIC_TOOL,
               ENTITY_CONTEXT_TOOL, RELATION_CHAIN_TOOL]


def _agent_tools(agent: str) -> list[dict]:
    """非 data 智能体的工具集（chat-native：均含 show_* 渲染卡片工具）。"""
    tools: list[dict] = list(SHOW_TOOLS)
    if agent == "task":
        tools += [PUBLISH_TASK_TOOL]
    tools += WRITE_TOOLS  # 所有智能体都可保存受众/策展知识（兜底，防路由误判）
    return tools


def _local_exec(name: str, args: dict, tid: int):
    """本地工具执行：命中返回 (result, meta)；非本地工具返回 (None, None)。
    meta 可带 task / created / navigate，供 /chat 汇总。"""
    if name == "publish_task":
        r = publish_task_handler(tid, args.get("task_name", "未命名任务"), args.get("source_object", "user"))
        return r, {"task": r}
    if name == "create_chart":
        r = create_chart_handler(tid, args.get("question", ""))
        return r, {"created": {"kind": "chart", "id": r["chart_id"], "title": r["title"], "path": "/analyst"}}
    if name == "create_dashboard":
        r = create_dashboard_handler(tid, args.get("question", ""))
        return r, {"created": {"kind": "dashboard", "id": r["dashboard_id"], "title": r["title"], "path": r["path"]}}
    if name == "save_audience":
        r = save_audience_handler(tid, args.get("name", ""), args.get("query", ""))
        return r, {}
    if name == "curate_knowledge":
        r = curate_knowledge_handler(tid, args.get("query", ""), args.get("in_context", True))
        return r, {}
    if name == "explain_metric":
        r = explain_metric_handler(tid, args.get("query", ""))
        return r, {}
    if name == "entity_context":
        r = entity_context_handler(tid, args.get("object", ""), str(args.get("id", "")))
        return r, {}
    if name == "relation_chain":
        obj, id_ = args.get("object", ""), str(args.get("id", ""))
        hops = int(args.get("max_hops", 3) or 3)
        r = relation_chain_handler(tid, obj, id_, hops)
        return r, {"view": {"type": "chain", "object": obj, "id": id_, "max_hops": hops}}
    if name == "open_page":
        path = (args.get("path") or "").strip()
        if path in PAGE_INDEX:
            return ({"opened": path, "name": PAGE_INDEX[path]}, {"navigate": {"path": path, "name": PAGE_INDEX[path]}})
        return {"error": f"未知页面：{path}（请从页面表里选）"}, {}
    # 渲染指令工具：不取数，只回一条 view 指令，前端据此渲染内联卡片
    if name == "show_profile":
        oid = args.get("one_id")
        return {"shown": "profile", "one_id": oid}, {"view": {"type": "profile", "one_id": oid}}
    if name == "show_audience":
        q = (args.get("query") or "").strip()
        return {"shown": "audience", "query": q}, {"view": {"type": "audience", "query": q}}
    if name == "show_table":
        obj = (args.get("object") or "user").strip()
        q = (args.get("query") or "").strip()
        return {"shown": "table", "object": obj, "query": q}, {"view": {"type": "table", "object": obj, "query": q}}
    if name == "show_chart":
        q = (args.get("question") or "").strip()
        return {"shown": "chart", "question": q}, {"view": {"type": "chart", "question": q}}
    return None, None


def _complete_run_later(run_id: str, tenant_id: int, entry: dict) -> None:
    try:
        time.sleep(3)
        with httpx.Client(timeout=30.0, trust_env=False, headers=SQL_HEADERS) as client:
            client.post(f"{SQL_ENGINE_URL}/connections/reverse-etl/runs/{run_id}/complete",
                        params={"tenant_id": tenant_id})
        entry["status"] = "success"
    except Exception:  # noqa: BLE001
        entry["status"] = "failed"


def publish_task_handler(tenant_id: int, task_name: str, source_object: str = "user") -> dict:
    with httpx.Client(timeout=30.0, trust_env=False, headers=SQL_HEADERS) as client:
        job = client.post(f"{SQL_ENGINE_URL}/connections/reverse-etl/jobs", params={"tenant_id": tenant_id},
                          json={"job_name": task_name, "source_object": source_object,
                                "destination_id": "assistant-demo", "schedule_cron": "0 */15 * * * *",
                                "enabled": True}).json()
        job_id = job.get("job_id") or job.get("id")
        run = client.post(f"{SQL_ENGINE_URL}/connections/reverse-etl/jobs/{job_id}/run-now",
                          params={"tenant_id": tenant_id}).json()
        run_id = run.get("run_id") or run.get("id")
    entry = {"run_id": run_id, "job_id": job_id, "task_name": task_name,
             "source_object": source_object, "tenant_id": tenant_id, "status": "running"}
    _TASK_STORE.insert(0, entry)
    threading.Thread(target=_complete_run_later, args=(run_id, tenant_id, entry), daemon=True).start()
    return {"run_id": run_id, "job_id": job_id, "status": "running", "task_name": task_name}


def create_chart_handler(tenant_id: int, question: str) -> dict:
    with httpx.Client(timeout=40.0, trust_env=False, headers=SQL_HEADERS) as c:
        spec = c.post(f"{SQL_ENGINE_URL}/analyst/charts/nl", params={"tenant_id": tenant_id},
                      json={"question": question}).json()
        saved = c.post(f"{SQL_ENGINE_URL}/analyst/charts", params={"tenant_id": tenant_id},
                       json={"title": spec["title"], "type": spec["type"], "source": spec["source"]}).json()
    return {"chart_id": saved["id"], "title": saved["title"], "type": saved["type"], "source": saved["source"]}


def save_audience_handler(tenant_id: int, name: str, query: str) -> dict:
    with httpx.Client(timeout=45.0, trust_env=False, headers=SQL_HEADERS) as c:
        draft = c.post(f"{SQL_ENGINE_URL}/agent/segment/draft",
                       json={"tenant_id": tenant_id, "question": query}).json()
        rule = draft.get("rule")
        clar = draft.get("clarifications") or []
        if not rule:
            return {"saved": False, "need_clarification": clar or ["未能从描述中解析出筛选条件，请更具体描述对象与条件。"]}
        code = f"seg_{int(time.time() * 1000) % 10**9:09d}"
        resp = c.post(f"{SQL_ENGINE_URL}/agent/segment/confirm",
                      json={"tenant_id": tenant_id, "segment_code": code,
                            "segment_name": name or "未命名受众", "rule": rule})
        if resp.status_code not in (200, 201):
            return {"saved": False, "error": resp.text[:200]}
    est = draft.get("estimate")
    if isinstance(est, dict):
        est = est.get("count")
    return {"saved": True, "segment_code": code, "segment_name": name or "未命名受众",
            "estimate": est, "echo": draft.get("echo")}


def explain_metric_handler(tenant_id: int, query: str) -> dict:
    with httpx.Client(timeout=30.0, trust_env=False, headers=SQL_HEADERS) as c:
        return c.get(f"{SQL_ENGINE_URL}/semantic/explain",
                     params={"tenant_id": tenant_id, "q": query}).json()


def entity_context_handler(tenant_id: int, obj: str, id_: str) -> dict:
    with httpx.Client(timeout=30.0, trust_env=False, headers=SQL_HEADERS) as c:
        return c.get(f"{SQL_ENGINE_URL}/semantic/entity",
                     params={"tenant_id": tenant_id, "object": obj, "id": id_}).json()


def relation_chain_handler(tenant_id: int, obj: str, id_: str, max_hops: int = 3) -> dict:
    with httpx.Client(timeout=30.0, trust_env=False, headers=SQL_HEADERS) as c:
        return c.get(f"{SQL_ENGINE_URL}/semantic/chain",
                     params={"tenant_id": tenant_id, "object": obj, "id": id_, "max_hops": max_hops}).json()


def curate_knowledge_handler(tenant_id: int, query: str, in_context: bool = True) -> dict:
    with httpx.Client(timeout=30.0, trust_env=False, headers=SQL_HEADERS) as c:
        files = c.get(f"{SQL_ENGINE_URL}/kb/files",
                      params={"tenant_id": tenant_id, "q": query}).json().get("files", [])
        if not files:
            return {"ok": False, "error": f"知识库没有匹配「{query}」的文件"}
        touched = []
        for f in files[:20]:
            c.post(f"{SQL_ENGINE_URL}/kb/files/{f['id']}/context",
                   json={"tenant_id": tenant_id, "in_context": bool(in_context)})
            touched.append(f["name"])
    return {"ok": True, "in_context": bool(in_context), "count": len(touched), "files": touched}


def create_dashboard_handler(tenant_id: int, question: str) -> dict:
    with httpx.Client(timeout=40.0, trust_env=False, headers=SQL_HEADERS) as c:
        spec = c.post(f"{SQL_ENGINE_URL}/analyst/dashboards/nl", params={"tenant_id": tenant_id},
                      json={"question": question}).json()
        saved = c.post(f"{SQL_ENGINE_URL}/analyst/dashboards", params={"tenant_id": tenant_id},
                       json={"title": spec["title"], "sources": spec["sources"]}).json()
    return {"dashboard_id": saved["id"], "title": saved["title"],
            "chart_count": len(saved.get("sources") or []),
            "path": f"/analyst/dashboards/custom/{saved['id']}"}


# ── DeepSeek ────────────────────────────────────────────────────────────────
async def _deepseek_chat(messages: list[dict], tools: list[dict] | None = None) -> dict:
    payload: dict = {"model": DEEPSEEK_MODEL, "messages": messages, "temperature": 0.2}
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"
    async with httpx.AsyncClient(timeout=60.0, trust_env=False) as client:
        resp = await client.post(f"{DEEPSEEK_API_BASE}/chat/completions",
                                 headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                                          "Content-Type": "application/json"}, json=payload)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]


def _summarize(result: Any, limit: int = 300) -> str:
    try:
        text = json.dumps(result, ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        text = str(result)
    return text if len(text) <= limit else text[:limit] + "…"


def _route_keyword(text: str) -> str:
    t = text or ""
    # 导航意图优先（general 持有 open_page）：打开/前往某页面，而非新建
    if any(w in t for w in ("打开", "前往", "跳转", "带我", "进入", "导航", "切到", "切换到", "去到")) \
            and not any(w in t for w in ("做一个", "做个", "创建", "新建", "生成", "建一个", "建个")):
        return "general"
    if any(w in t for w in ("看板", "图表", "饼图", "柱状", "趋势", "占比", "分布图", "可视化", "dashboard", "chart")):
        return "analyst"
    if any(w in t for w in ("发布任务", "跑批", "同步", "导出", "运行任务", "后台任务", "调度")):
        return "task"
    if any(w in t for w in ("多少", "查", "列出", "搜索", "画像", "受众", "标签", "订单", "客户", "线索", "用户")):
        return "data"
    return "general"


async def _route(messages: list[ChatMessage]) -> str:
    last = next((m.content for m in reversed(messages) if m.role == "user"), "")
    fallback = _route_keyword(last)
    if not DEEPSEEK_API_KEY:
        return fallback
    catalog = "\n".join(f"- {k}: {v['desc']}" for k, v in AGENT_DEFS.items())
    sysmsg = ("你是多智能体路由器。把用户的最新请求分派给最合适的智能体，只返回 JSON："
              "{\"agent\":\"data|analyst|task|general\"}。\n智能体：\n" + catalog)
    try:
        out = await _deepseek_chat([{"role": "system", "content": sysmsg},
                                    {"role": "user", "content": last}])
        # 没给 tools，直接读 content 里的 JSON
        content = out.get("content") or "{}"
        agent = (json.loads(content).get("agent") or "").strip()
        return agent if agent in AGENT_DEFS else fallback
    except Exception:  # noqa: BLE001
        return fallback


# ── 端点 ────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health() -> dict:
    mcp_count = 0
    try:
        schemas = _TOOL_SCHEMA_CACHE if _TOOL_SCHEMA_CACHE is not None else await _fetch_tool_schemas()
        mcp_count = len(schemas)
    except Exception:  # noqa: BLE001
        mcp_count = 0
    return {"status": "ok", "llm": bool(DEEPSEEK_API_KEY), "mcp_tools": mcp_count,
            "agents": [{"key": k, "name": v["name"], "desc": v["desc"]} for k, v in AGENT_DEFS.items()]}


@app.get("/agents")
async def agents() -> dict:
    return {"agents": [{"key": k, "name": v["name"], "desc": v["desc"]} for k, v in AGENT_DEFS.items()]}


@app.get("/mcp/tools")
async def mcp_tools() -> dict:
    server = {"name": "dataagent-cdp", "transport": "stdio", "path": MCP_SERVER_PATH}
    try:
        schemas = _TOOL_SCHEMA_CACHE if _TOOL_SCHEMA_CACHE is not None else await _fetch_tool_schemas()
    except Exception as e:  # noqa: BLE001
        return {"server": server, "tools": [], "error": str(e)}
    tools = [{"name": s["function"]["name"], "description": s["function"]["description"],
              "parameters": s["function"]["parameters"]} for s in schemas]
    return {"server": server, "tools": tools}


async def _agent_loop(messages: list[dict], tools: list[dict], execute) -> tuple[str, list, dict | None, dict | None, dict | None, list]:
    """通用 tool-call 循环。execute(name, args) -> (result, meta{task?,created?,navigate?,view?})。"""
    steps: list[dict] = []
    task: dict | None = None
    created: dict | None = None
    navigate: dict | None = None
    views: list[dict] = []  # chat-native：要渲染的内联卡片指令（按出现顺序）
    reply = ""
    for _ in range(MAX_TOOL_ITERS):
        message = await _deepseek_chat(messages, tools or None)
        tcs = message.get("tool_calls")
        if not tcs:
            reply = message.get("content") or ""
            break
        messages.append({"role": "assistant", "content": message.get("content") or "", "tool_calls": tcs})
        for tc in tcs:
            name = tc["function"]["name"]
            raw = tc["function"].get("arguments") or "{}"
            try:
                args = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:  # noqa: BLE001
                args = {}
            ok = True
            try:
                result, meta = await execute(name, args)
                if meta.get("task"):
                    task = meta["task"]
                if meta.get("created"):
                    created = meta["created"]
                if meta.get("navigate"):
                    navigate = meta["navigate"]
                if meta.get("view"):
                    views.append(meta["view"])
            except Exception as e:  # noqa: BLE001
                ok = False
                result, meta = {"error": str(e)}, {}
            messages.append({"role": "tool", "tool_call_id": tc.get("id"),
                             "content": json.dumps(result, ensure_ascii=False, default=str)})
            steps.append({"tool": name, "args": args, "ok": ok, "summary": _summarize(result)})
    else:
        reply = reply or "（已达到工具调用上限，部分结果见 steps）"
    return reply, steps, task, created, navigate, views


@app.post("/chat")
async def chat(req: ChatRequest) -> dict:
    if not DEEPSEEK_API_KEY:
        return {"reply": "（未配置 DeepSeek API Key，智能助手暂不可用）", "agent": "general",
                "agent_name": AGENT_DEFS["general"]["name"], "steps": [], "task": None,
                "created": None, "navigate": None, "views": []}

    tid = req.tenant_id
    mode = (req.mode or "agent").lower()

    # ask 模式：只回答/解释，不路由专职体、不调用任何工具、不渲染卡片。
    if mode == "ask":
        ask_sys = {"role": "system", "content": AGENT_SYSTEM["general"].format(tenant_id=tid)
                   + "\n\n【提问模式】用户只想获得回答与解释，请直接、简洁作答，不要执行任何操作，也不要声称已渲染卡片或打开页面。"}
        messages = [ask_sys] + [{"role": m.role, "content": m.content} for m in req.messages]
        base = {"agent": "general", "agent_name": "提问"}
        try:
            reply = (await _deepseek_chat(messages)).get("content") or ""
        except Exception as e:  # noqa: BLE001
            reply = f"（智能助手处理出错：{e}）"
        _persist_turn(req, reply, "ask")
        return {**base, "reply": reply, "steps": [], "task": None, "created": None, "navigate": None, "views": []}

    agent = await _route(req.messages)
    sysmsg = {"role": "system", "content": AGENT_SYSTEM[agent].format(tenant_id=tid) + SHOW_SUFFIX}
    messages: list[dict] = [sysmsg] + [{"role": m.role, "content": m.content} for m in req.messages]
    base = {"agent": agent, "agent_name": AGENT_DEFS[agent]["name"]}

    try:
        if agent == "data":
            async with stdio_client(_mcp_params()) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    mcp_list = (await session.list_tools()).tools
                    global _TOOL_SCHEMA_CACHE
                    _TOOL_SCHEMA_CACHE = [_mcp_tool_to_function(t) for t in mcp_list]
                    names = {t.name for t in mcp_list}
                    tools = _TOOL_SCHEMA_CACHE + SHOW_TOOLS + WRITE_TOOLS  # data 也可渲染卡片 + 写操作

                    async def execute(name, args):
                        res, meta = _local_exec(name, args, tid)
                        if res is not None:
                            return res, meta
                        if name in names:
                            return _extract_mcp_result(await session.call_tool(name, args)), {}
                        return {"error": f"未知工具：{name}"}, {}

                    reply, steps, task, created, navigate, views = await _agent_loop(messages, tools, execute)
        else:
            async def execute(name, args):
                res, meta = _local_exec(name, args, tid)
                if res is not None:
                    return res, meta
                return {"error": f"未知工具：{name}"}, {}

            reply, steps, task, created, navigate, views = await _agent_loop(messages, _agent_tools(agent), execute)

        _persist_turn(req, reply, agent)
        return {**base, "reply": reply, "steps": steps, "task": task, "created": created, "navigate": navigate, "views": views}
    except Exception as e:  # noqa: BLE001
        reply = f"（智能助手处理出错：{e}）"
        _persist_turn(req, reply, agent)
        return {**base, "reply": reply, "steps": [], "task": None, "created": None, "navigate": None, "views": []}


def _persist_turn(req: ChatRequest, reply: str, agent: str) -> None:
    """保存本轮：最新用户消息 + 助手回复（按 user_id + conversation_id）。"""
    last_user = next((m.content for m in reversed(req.messages) if m.role == "user"), "")
    rows = []
    if last_user:
        rows.append(("user", last_user, None))
    rows.append(("assistant", reply, agent))
    _save_messages(req.tenant_id, req.user_id, req.conversation_id, rows)


@app.get("/conversations")
async def conversations(user_id: int, tenant_id: int, limit: int = 50) -> dict:
    """会话列表（new chat / 会话保存）。"""
    return {"conversations": _list_conversations(tenant_id, user_id, limit)}


@app.get("/history")
async def history(user_id: int, tenant_id: int, conversation_id: str | None = None, limit: int = 50) -> dict:
    return {"messages": _load_history(tenant_id, user_id, conversation_id, limit)}


@app.delete("/history")
async def clear_history(user_id: int, tenant_id: int, conversation_id: str | None = None) -> dict:
    """删除会话：给 conversation_id 则只删该会话，否则清空该用户全部。"""
    try:
        with _db() as conn, conn.cursor() as cur:
            if conversation_id:
                cur.execute("DELETE FROM assistant_messages WHERE tenant_id=%s AND user_id=%s AND conversation_id=%s",
                            (tenant_id, user_id, conversation_id))
            else:
                cur.execute("DELETE FROM assistant_messages WHERE tenant_id=%s AND user_id=%s", (tenant_id, user_id))
    except Exception:  # noqa: BLE001
        pass
    return {"ok": True}


@app.get("/tasks")
async def list_tasks() -> dict:
    return {"tasks": list(_TASK_STORE)}


@app.get("/tasks/{run_id}")
async def get_task(run_id: str) -> dict:
    for t in _TASK_STORE:
        if str(t.get("run_id")) == str(run_id):
            return t
    raise HTTPException(status_code=404, detail="task not found")


# ── 主动式埋点 Copilot ────────────────────────────────────────────────────────
# 链路：前端 tracker 缓冲行为 → 触发批量 POST /observe → 落库 + 启发式门控 +
# copilot agent 判断是否主动建议 → 有建议时前端自动展开侧边栏弹出。
# 两层门控（前端触发点 + 后端启发式）确保 LLM 调用稀疏、成本可控。

class BehaviorEvent(BaseModel):
    type: str                       # page_view/click/search/empty_state/error/idle/repeat
    path: str | None = None
    name: str | None = None
    ts: int | None = None
    payload: dict | None = None


class ObserveRequest(BaseModel):
    tenant_id: int
    user_id: int | None = None
    session_id: str
    page: dict | None = None        # {path, name}
    events: list[BehaviorEvent] = []


# 高价值页面 → 首次进入时的贴心提示（title, message）
PAGE_TIPS: dict[str, tuple[str, str]] = {
    "/accounts/-/merge-log": ("OneID 合并", "需要我解释这条记录的 OneID 合并规则，或帮你查某个用户的合并历史吗？"),
    "/unify/identity": ("身份解析", "想了解 channel→OneID 的合并逻辑，或调整身份规则吗？我可以帮你解释。"),
    "/connections/flow": ("可视化编排", "要我帮你梳理这个 ETL 编排，或基于现有节点跑一条管道吗？"),
    "/unify/predictions": ("预测模型", "需要我解释购买/流失/LTV 这几个预测分数怎么用来圈人吗？"),
}
CREATE_PAGES = {"/engage/audiences/new", "/filter"}  # 新建分群


def _save_behavior(req: ObserveRequest) -> None:
    if not req.user_id or not req.events:
        return
    page = req.page or {}
    try:
        rows = []
        for e in req.events:
            path = e.path or page.get("path")
            name = e.name or page.get("name") or PAGE_INDEX.get(path or "")
            rows.append((req.tenant_id, req.user_id, req.session_id, e.type, path, name,
                         json.dumps(e.payload or {}, ensure_ascii=False)))
        with _db() as conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO user_behavior_events "
                "(tenant_id,user_id,session_id,event_type,page_path,page_name,payload) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)", rows)
    except Exception:  # noqa: BLE001
        pass


def _save_suggestion(req: ObserveRequest, signal: str, sug: dict) -> None:
    if not req.user_id:
        return
    try:
        with _db() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO proactive_suggestions "
                "(tenant_id,user_id,session_id,trigger_signal,title,message,action,confidence) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (req.tenant_id, req.user_id, req.session_id, signal, sug.get("title"),
                 sug.get("message"), json.dumps(sug.get("action") or {}, ensure_ascii=False),
                 sug.get("confidence")))
    except Exception:  # noqa: BLE001
        pass


def _detect_signal(req: ObserveRequest) -> tuple[str | None, str]:
    """启发式门控：从本批行为中识别「值得主动出手」的信号。无信号返回 (None, '')。"""
    events = req.events
    types = {e.type for e in events}
    page_path = (req.page or {}).get("path") or ""
    if "error" in types:
        return "error", page_path
    if "empty_state" in types:
        return "empty_result", page_path
    if "repeat" in types:
        return "repeat", page_path
    if "idle" in types:
        if page_path in CREATE_PAGES:
            return "stuck_creating", page_path
        if page_path.startswith("/analyst"):
            return "stuck_analyst", page_path
        # 任意页面长时间停留无操作 → 主动问询（结合页面用途）
        return "dwell", page_path
    # 首次进入高价值页（page_view 命中提示页）
    for e in events:
        if e.type == "page_view" and (e.path or "") in PAGE_TIPS:
            return "explain_page", e.path or ""
    return None, ""


def _template_suggestion(signal: str, path: str) -> dict | None:
    """无 LLM / LLM 失败时的固定文案降级（与 AGENT_LLM_ENABLED=0 降级思路一致）。"""
    # 高价值页有专属文案时优先用（首次进入 explain_page，或在该页停留 dwell）
    if signal in ("explain_page", "dwell") and path in PAGE_TIPS:
        title, msg = PAGE_TIPS[path]
        return {"title": title, "message": msg, "action": {"type": "none"}, "confidence": 0.6}
    if signal == "dwell":
        return {"title": "需要帮忙吗？", "message": "在这页停留一会儿了，要我帮你查点什么、或解释下这个页面能做什么吗？",
                "action": {"type": "none"}, "confidence": 0.55}
    base = {
        "error": {"title": "出错了？", "message": "刚才那步好像出错了，要我帮你排查或换个条件再试吗？",
                  "action": {"type": "prefill", "text": "帮我看看刚才为什么出错"}},
        "empty_result": {"title": "没有匹配结果", "message": "条件可能太严了。用一句话描述你想圈的人群，我帮你生成分群并预估规模。",
                         "action": {"type": "prefill", "text": "帮我建一个分群："}},
        "stuck_creating": {"title": "需要帮忙建分群？", "message": "直接告诉我你想圈的人群，我来生成 DSL 并预估规模。",
                           "action": {"type": "prefill", "text": "帮我建一个分群："}},
        "stuck_analyst": {"title": "需要帮忙做图？", "message": "描述你想看的指标，我直接帮你建图表或看板。",
                          "action": {"type": "prefill", "text": "帮我做一个图表："}},
        "repeat": {"title": "在找什么功能？", "message": "告诉我你想做什么，我带你过去或直接帮你完成。",
                   "action": {"type": "none"}},
    }
    s = base.get(signal)
    if s:
        s = {**s, "confidence": 0.6}
    return s


COPILOT_SYSTEM = (
    "你是 AgenticDataHub CDP 控制台的「主动助手」。根据用户在控制台的最近行为和当前所在页面，"
    "判断是否需要主动给用户一条具体、可执行的建议。不确定、或此刻打扰弊大于利时，保持沉默。"
    "只输出 JSON，不要任何多余文字，格式："
    "{\"should_suggest\":true/false,\"title\":\"<=12字\",\"message\":\"<=60字，具体可执行\","
    "\"action\":{\"type\":\"open_page|prefill|none\",\"path\":\"open_page时的页面路由\",\"text\":\"prefill时预填给聊天框的话\"},"
    "\"confidence\":0~1}。open_page 的 path 必须取自给定页面表，勿编造。"
    "语气像贴心的同事，简短友好，不要营销腔。"
)


async def _copilot_suggest(req: ObserveRequest, signal: str, path: str) -> dict | None:
    tmpl = _template_suggestion(signal, path)
    if not DEEPSEEK_API_KEY:
        return tmpl
    page_name = (req.page or {}).get("name") or PAGE_INDEX.get(path, path)
    ev_lines = "; ".join(
        f"{e.type}@{e.path or path}" + (f"({_summarize(e.payload, 80)})" if e.payload else "")
        for e in req.events[-12:])
    pages = "\n".join(f"- {p} — {label}" for p, label in PAGE_CATALOG)
    nudge = ("这是『在该页停留较久且无操作』信号——请主动、简短地问候一句，"
             "结合本页用途给出具体的帮助方向（通常 should_suggest=true；除非是登录页等不宜打扰的场景）。"
             if signal in ("dwell", "stuck_creating", "stuck_analyst") else
             "若此刻打扰用户弊大于利，should_suggest 设为 false。")
    user = (f"当前页面：{path}（{page_name}）。\n命中信号：{signal}。\n最近行为：{ev_lines}\n"
            f"参考建议（可改写或否决）：{(tmpl or {}).get('message', '')}\n可用页面路由：\n{pages}\n{nudge}")
    try:
        msg = await _deepseek_chat([{"role": "system", "content": COPILOT_SYSTEM},
                                    {"role": "user", "content": user}])
        content = (msg.get("content") or "").strip()
        content = re.sub(r"^```(?:json)?|```$", "", content).strip()
        data = json.loads(content)
        if not data.get("should_suggest"):
            return None
        act = data.get("action") or {"type": "none"}
        if act.get("type") == "open_page" and act.get("path") not in PAGE_INDEX:
            act = {"type": "none"}
        return {"title": data.get("title") or (tmpl or {}).get("title") or "建议",
                "message": data.get("message") or (tmpl or {}).get("message") or "",
                "action": act, "confidence": float(data.get("confidence") or 0.6)}
    except Exception:  # noqa: BLE001
        return tmpl


@app.post("/observe")
async def observe(req: ObserveRequest) -> dict:
    """接收一批浏览器行为：落库 + 启发式门控 + copilot 判断。返回 {suggestion: {...}|null}。"""
    if not PROACTIVE_ENABLED:
        return {"suggestion": None}
    _save_behavior(req)
    signal, path = _detect_signal(req)
    if not signal:
        return {"suggestion": None}
    now = time.time()
    if now - _SUGGEST_COOLDOWN.get(req.session_id, 0.0) < PROACTIVE_COOLDOWN_SEC:
        return {"suggestion": None}  # 冷却期内不打扰，也不烧 token
    try:
        sug = await _copilot_suggest(req, signal, path)
    except Exception:  # noqa: BLE001
        sug = None
    if not sug:
        return {"suggestion": None}
    _SUGGEST_COOLDOWN[req.session_id] = now
    sug["signal"] = signal
    _save_suggestion(req, signal, sug)
    return {"suggestion": sug}

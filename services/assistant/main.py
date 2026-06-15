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
MCP_SERVER_PATH = os.getenv("MCP_SERVER_PATH", "/app/mcp/server.py")
MCP_SQL_ENGINE_URL = os.getenv("MCP_SQL_ENGINE_URL", SQL_ENGINE_URL)

MAX_TOOL_ITERS = 5

_TOOL_SCHEMA_CACHE: list[dict] | None = None
_TASK_STORE: list[dict] = []

# ── 聊天记录持久化（按用户）─────────────────────────────────────────────────
MYSQL_CFG = {
    "host": os.getenv("MYSQL_HOST", "mysql"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "agenticdatahub"),
    "password": os.getenv("MYSQL_PASSWORD", "agenticdatahub123"),
    "database": os.getenv("MYSQL_DATABASE", "agenticdatahub"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}


def _db():
    return pymysql.connect(**MYSQL_CFG, autocommit=True)


def _save_messages(tenant_id: int, user_id: int | None, rows: list[tuple]) -> None:
    """rows: [(role, content, agent), ...]。无 user_id 或出错时静默跳过。"""
    if not user_id or not rows:
        return
    try:
        with _db() as conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO assistant_messages (tenant_id, user_id, role, content, agent) VALUES (%s,%s,%s,%s,%s)",
                [(tenant_id, user_id, r, c, a) for (r, c, a) in rows],
            )
    except Exception:  # noqa: BLE001
        pass


def _load_history(tenant_id: int, user_id: int | None, limit: int = 50) -> list[dict]:
    if not user_id:
        return []
    try:
        with _db() as conn, conn.cursor() as cur:
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

# ── 智能体定义 ──────────────────────────────────────────────────────────────
AGENT_DEFS: dict[str, dict] = {
    "data":    {"name": "数据查询", "desc": "查询底座数据：用户/线索/客户/订单/受众/标签/画像等"},
    "analyst": {"name": "分析",     "desc": "创建图表或看板、出指标分布（电商/线索/客户等）"},
    "task":    {"name": "任务",     "desc": "发布/运行后台任务，如同步受众、导出、跑批"},
    "general": {"name": "通用",     "desc": "产品介绍、使用答疑、打开/前往/跳转到某功能页面（导航）、其它对话"},
}

AGENT_SYSTEM: dict[str, str] = {
    "data": (
        "你是 AgenticDataHub 的「数据查询」智能体；通过只读 MCP 工具查询「智能实时数据底座」"
        "（用户/线索/客户/订单/受众/标签/画像等）。当前 tenant_id 是 {tenant_id}，调用需要 tenant_id 的工具时务必带上。"
        "回答简洁，用用户的语言。"
    ),
    "analyst": (
        "你是 AgenticDataHub 的「分析」智能体。用户想要图表/看板/指标时：单个图表用 `create_chart`，"
        "一个含多图的看板用 `create_dashboard`，把用户需求原样作为 question 传给工具。当前 tenant_id 是 {tenant_id}。"
        "注意：若用户是「打开/前往某个已有看板页面」（导航），应调用 `open_page` 跳转，**不要新建**；只有要新看板时才 create_dashboard。"
        "建好后简要说明名称，并提示去「分析」查看。回答简洁。"
    ),
    "task": (
        "你是 AgenticDataHub 的「任务」智能体。用户要发布/运行后台任务（同步受众、导出、跑批等）时调用 `publish_task`。"
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


def _agent_tools(agent: str) -> list[dict]:
    """非 data 智能体的工具集（均含 open_page 导航）。"""
    tools: list[dict] = []
    if agent == "analyst":
        tools += [CREATE_CHART_TOOL, CREATE_DASHBOARD_TOOL]
    elif agent == "task":
        tools += [PUBLISH_TASK_TOOL]
    tools.append(OPEN_PAGE_TOOL)
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
    if name == "open_page":
        path = (args.get("path") or "").strip()
        if path in PAGE_INDEX:
            return ({"opened": path, "name": PAGE_INDEX[path]}, {"navigate": {"path": path, "name": PAGE_INDEX[path]}})
        return {"error": f"未知页面：{path}（请从页面表里选）"}, {}
    return None, None


def _complete_run_later(run_id: str, tenant_id: int, entry: dict) -> None:
    try:
        time.sleep(3)
        with httpx.Client(timeout=30.0, trust_env=False) as client:
            client.post(f"{SQL_ENGINE_URL}/connections/reverse-etl/runs/{run_id}/complete",
                        params={"tenant_id": tenant_id})
        entry["status"] = "success"
    except Exception:  # noqa: BLE001
        entry["status"] = "failed"


def publish_task_handler(tenant_id: int, task_name: str, source_object: str = "user") -> dict:
    with httpx.Client(timeout=30.0, trust_env=False) as client:
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
    with httpx.Client(timeout=40.0, trust_env=False) as c:
        spec = c.post(f"{SQL_ENGINE_URL}/analyst/charts/nl", params={"tenant_id": tenant_id},
                      json={"question": question}).json()
        saved = c.post(f"{SQL_ENGINE_URL}/analyst/charts", params={"tenant_id": tenant_id},
                       json={"title": spec["title"], "type": spec["type"], "source": spec["source"]}).json()
    return {"chart_id": saved["id"], "title": saved["title"], "type": saved["type"], "source": saved["source"]}


def create_dashboard_handler(tenant_id: int, question: str) -> dict:
    with httpx.Client(timeout=40.0, trust_env=False) as c:
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
    server = {"name": "agenticdatahub-cdp", "transport": "stdio", "path": MCP_SERVER_PATH}
    try:
        schemas = _TOOL_SCHEMA_CACHE if _TOOL_SCHEMA_CACHE is not None else await _fetch_tool_schemas()
    except Exception as e:  # noqa: BLE001
        return {"server": server, "tools": [], "error": str(e)}
    tools = [{"name": s["function"]["name"], "description": s["function"]["description"],
              "parameters": s["function"]["parameters"]} for s in schemas]
    return {"server": server, "tools": tools}


async def _agent_loop(messages: list[dict], tools: list[dict], execute) -> tuple[str, list, dict | None, dict | None, dict | None]:
    """通用 tool-call 循环。execute(name, args) -> (result, meta{task?,created?,navigate?})。"""
    steps: list[dict] = []
    task: dict | None = None
    created: dict | None = None
    navigate: dict | None = None
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
            except Exception as e:  # noqa: BLE001
                ok = False
                result, meta = {"error": str(e)}, {}
            messages.append({"role": "tool", "tool_call_id": tc.get("id"),
                             "content": json.dumps(result, ensure_ascii=False, default=str)})
            steps.append({"tool": name, "args": args, "ok": ok, "summary": _summarize(result)})
    else:
        reply = reply or "（已达到工具调用上限，部分结果见 steps）"
    return reply, steps, task, created, navigate


@app.post("/chat")
async def chat(req: ChatRequest) -> dict:
    if not DEEPSEEK_API_KEY:
        return {"reply": "（未配置 DeepSeek API Key，智能助手暂不可用）", "agent": "general",
                "agent_name": AGENT_DEFS["general"]["name"], "steps": [], "task": None,
                "created": None, "navigate": None}

    agent = await _route(req.messages)
    tid = req.tenant_id
    sysmsg = {"role": "system", "content": AGENT_SYSTEM[agent].format(tenant_id=tid) + NAV_SUFFIX}
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
                    tools = _TOOL_SCHEMA_CACHE + [OPEN_PAGE_TOOL]  # data 也可导航

                    async def execute(name, args):
                        res, meta = _local_exec(name, args, tid)
                        if res is not None:
                            return res, meta
                        if name in names:
                            return _extract_mcp_result(await session.call_tool(name, args)), {}
                        return {"error": f"未知工具：{name}"}, {}

                    reply, steps, task, created, navigate = await _agent_loop(messages, tools, execute)
        else:
            async def execute(name, args):
                res, meta = _local_exec(name, args, tid)
                if res is not None:
                    return res, meta
                return {"error": f"未知工具：{name}"}, {}

            reply, steps, task, created, navigate = await _agent_loop(messages, _agent_tools(agent), execute)

        _persist_turn(req, reply, agent)
        return {**base, "reply": reply, "steps": steps, "task": task, "created": created, "navigate": navigate}
    except Exception as e:  # noqa: BLE001
        reply = f"（智能助手处理出错：{e}）"
        _persist_turn(req, reply, agent)
        return {**base, "reply": reply, "steps": [], "task": None, "created": None, "navigate": None}


def _persist_turn(req: ChatRequest, reply: str, agent: str) -> None:
    """保存本轮：最新用户消息 + 助手回复（按 user_id）。"""
    last_user = next((m.content for m in reversed(req.messages) if m.role == "user"), "")
    rows = []
    if last_user:
        rows.append(("user", last_user, None))
    rows.append(("assistant", reply, agent))
    _save_messages(req.tenant_id, req.user_id, rows)


@app.get("/history")
async def history(user_id: int, tenant_id: int, limit: int = 50) -> dict:
    return {"messages": _load_history(tenant_id, user_id, limit)}


@app.delete("/history")
async def clear_history(user_id: int, tenant_id: int) -> dict:
    try:
        with _db() as conn, conn.cursor() as cur:
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

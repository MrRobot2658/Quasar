"""AgenticDataHub「智能助手」聊天后端。

职责：
  1) 调 DeepSeek（OpenAI 兼容 chat/completions + function-calling）。
  2) 桥接已有的 CDP MCP server（stdio），把 MCP 只读工具暴露给 LLM 调用。
  3) 「发布任务」：调 sql-engine 的 reverse-ETL 调度模拟，后台跑批并回写状态。

设计要点：
  - LLM 只通过工具读「智能实时数据底座」，不直接写。
  - 一次 /chat 只开一个 MCP ClientSession，在整个 tool-call 循环里复用。
  - MCP 不可用 / 无 DeepSeek Key 时降级，绝不 500，返回友好提示。
"""

import asyncio
import json
import os
import sys
import threading
import time
from typing import Any

import httpx
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

# ── 模块级缓存 / 内存态 ─────────────────────────────────────────────────────
_TOOL_SCHEMA_CACHE: list[dict] | None = None  # /mcp/tools 用：首次成功后缓存工具 schema 列表
_TASK_STORE: list[dict] = []  # 后台任务，最近的在前

app = FastAPI(title="AgenticDataHub 智能助手")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── 请求/响应模型 ───────────────────────────────────────────────────────────
class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    tenant_id: int
    messages: list[ChatMessage]


# ── MCP 桥接 ────────────────────────────────────────────────────────────────
def _mcp_params() -> StdioServerParameters:
    return StdioServerParameters(
        command=sys.executable,
        args=[MCP_SERVER_PATH],
        env={**os.environ, "SQL_ENGINE_URL": MCP_SQL_ENGINE_URL, "no_proxy": "*"},
    )


def _mcp_tool_to_function(t: Any) -> dict:
    """把一个 MCP 工具转成 DeepSeek function tool。"""
    return {
        "type": "function",
        "function": {
            "name": t.name,
            "description": t.description or "",
            "parameters": t.inputSchema or {"type": "object", "properties": {}},
        },
    }


async def _fetch_tool_schemas() -> list[dict]:
    """开一个 MCP 会话拉取工具清单，转为 DeepSeek function tools，并缓存。"""
    global _TOOL_SCHEMA_CACHE
    async with stdio_client(_mcp_params()) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = (await session.list_tools()).tools
            schemas = [_mcp_tool_to_function(t) for t in tools]
    _TOOL_SCHEMA_CACHE = schemas
    return schemas


# ── publish_task 本地工具 ───────────────────────────────────────────────────
PUBLISH_TASK_TOOL = {
    "type": "function",
    "function": {
        "name": "publish_task",
        "description": "发布一个后台任务（接入 reverse-ETL 调度模拟），立即返回任务ID，任务在后台运行",
        "parameters": {
            "type": "object",
            "properties": {
                "task_name": {"type": "string", "description": "任务名称"},
                "source_object": {
                    "type": "string",
                    "description": "源对象，如 user/lead/account/order",
                    "default": "user",
                },
            },
            "required": ["task_name"],
        },
    },
}


def _complete_run_later(run_id: str, tenant_id: int, entry: dict) -> None:
    """后台线程：sleep 3s 后通知 sql-engine 任务完成，并更新内存状态。"""
    try:
        time.sleep(3)
        with httpx.Client(timeout=30.0, trust_env=False) as client:
            client.post(
                f"{SQL_ENGINE_URL}/connections/reverse-etl/runs/{run_id}/complete",
                params={"tenant_id": tenant_id},
            )
        entry["status"] = "success"
    except Exception:  # noqa: BLE001
        entry["status"] = "failed"


def publish_task_handler(tenant_id: int, task_name: str, source_object: str = "user") -> dict:
    """发布任务：建 reverse-ETL job → run-now → 后台跑批 → 回写状态。"""
    with httpx.Client(timeout=30.0, trust_env=False) as client:
        job_resp = client.post(
            f"{SQL_ENGINE_URL}/connections/reverse-etl/jobs",
            params={"tenant_id": tenant_id},
            json={
                "job_name": task_name,
                "source_object": source_object,
                "destination_id": "assistant-demo",
                "schedule_cron": "0 */15 * * * *",
                "enabled": True,
            },
        )
        job_resp.raise_for_status()
        job = job_resp.json()
        job_id = job.get("job_id") or job.get("id")

        run_resp = client.post(
            f"{SQL_ENGINE_URL}/connections/reverse-etl/jobs/{job_id}/run-now",
            params={"tenant_id": tenant_id},
        )
        run_resp.raise_for_status()
        run = run_resp.json()
        run_id = run.get("run_id") or run.get("id")

    entry = {
        "run_id": run_id,
        "job_id": job_id,
        "task_name": task_name,
        "source_object": source_object,
        "tenant_id": tenant_id,
        "status": "running",
    }
    _TASK_STORE.insert(0, entry)

    threading.Thread(
        target=_complete_run_later, args=(run_id, tenant_id, entry), daemon=True
    ).start()

    return {
        "run_id": run_id,
        "job_id": job_id,
        "status": "running",
        "task_name": task_name,
    }


# ── DeepSeek 调用 ───────────────────────────────────────────────────────────
async def _deepseek_chat(messages: list[dict], tools: list[dict]) -> dict:
    """调 DeepSeek chat/completions，带 tools + tool_choice=auto，返回首个 message。"""
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": messages,
        "tools": tools,
        "tool_choice": "auto",
        "temperature": 0.2,
    }
    async with httpx.AsyncClient(timeout=60.0, trust_env=False) as client:
        resp = await client.post(
            f"{DEEPSEEK_API_BASE}/chat/completions",
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]


def _summarize(result: Any, limit: int = 300) -> str:
    """把工具结果压成简短文本（截断 ~limit 字）。"""
    try:
        text = json.dumps(result, ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        text = str(result)
    return text if len(text) <= limit else text[:limit] + "…"


SYSTEM_PROMPT_TMPL = (
    "你是 AgenticDataHub 的「智能助手」；可以通过工具查询「智能实时数据底座」里的数据"
    "（用户/线索/客户/订单/受众/标签等）；当前 tenant_id 是 {tenant_id}，调用需要 tenant_id 的工具时务必带上；"
    "当用户要求「发布/运行任务」（如同步受众、导出、跑批）时调用 `publish_task` 工具；"
    "回答简洁，用用户的语言。"
)


# ── 端点 ────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health() -> dict:
    mcp_count = 0
    try:
        schemas = _TOOL_SCHEMA_CACHE if _TOOL_SCHEMA_CACHE is not None else await _fetch_tool_schemas()
        mcp_count = len(schemas)
    except Exception:  # noqa: BLE001
        mcp_count = 0
    return {"status": "ok", "llm": bool(DEEPSEEK_API_KEY), "mcp_tools": mcp_count}


@app.get("/mcp/tools")
async def mcp_tools() -> dict:
    server = {"name": "agenticdatahub-cdp", "transport": "stdio", "path": MCP_SERVER_PATH}
    try:
        schemas = _TOOL_SCHEMA_CACHE if _TOOL_SCHEMA_CACHE is not None else await _fetch_tool_schemas()
    except Exception as e:  # noqa: BLE001
        return {"server": server, "tools": [], "error": str(e)}
    tools = [
        {
            "name": s["function"]["name"],
            "description": s["function"]["description"],
            "parameters": s["function"]["parameters"],
        }
        for s in schemas
    ]
    return {"server": server, "tools": tools}


@app.post("/chat")
async def chat(req: ChatRequest) -> dict:
    if not DEEPSEEK_API_KEY:
        return {
            "reply": "（未配置 DeepSeek API Key，智能助手暂不可用）",
            "steps": [],
            "task": None,
        }

    system_msg = {"role": "system", "content": SYSTEM_PROMPT_TMPL.format(tenant_id=req.tenant_id)}
    messages: list[dict] = [system_msg] + [{"role": m.role, "content": m.content} for m in req.messages]

    steps: list[dict] = []
    task: dict | None = None

    try:
        async with stdio_client(_mcp_params()) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                mcp_tool_list = (await session.list_tools()).tools
                global _TOOL_SCHEMA_CACHE
                _TOOL_SCHEMA_CACHE = [_mcp_tool_to_function(t) for t in mcp_tool_list]
                tools = _TOOL_SCHEMA_CACHE + [PUBLISH_TASK_TOOL]
                mcp_names = {t.name for t in mcp_tool_list}

                reply = ""
                for _ in range(MAX_TOOL_ITERS):
                    message = await _deepseek_chat(messages, tools)
                    tool_calls = message.get("tool_calls")
                    if not tool_calls:
                        reply = message.get("content") or ""
                        break

                    # 追加 assistant 的 tool_calls 消息
                    messages.append(
                        {
                            "role": "assistant",
                            "content": message.get("content") or "",
                            "tool_calls": tool_calls,
                        }
                    )

                    for tc in tool_calls:
                        name = tc["function"]["name"]
                        raw_args = tc["function"].get("arguments") or "{}"
                        try:
                            args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                        except Exception:  # noqa: BLE001
                            args = {}

                        ok = True
                        try:
                            if name == "publish_task":
                                result = publish_task_handler(
                                    req.tenant_id,
                                    args.get("task_name", "未命名任务"),
                                    args.get("source_object", "user"),
                                )
                                task = result
                            elif name in mcp_names:
                                mcp_res = await session.call_tool(name, args)
                                result = _extract_mcp_result(mcp_res)
                            else:
                                ok = False
                                result = {"error": f"未知工具：{name}"}
                        except Exception as e:  # noqa: BLE001
                            ok = False
                            result = {"error": str(e)}

                        messages.append(
                            {
                                "role": "tool",
                                "tool_call_id": tc.get("id"),
                                "content": json.dumps(result, ensure_ascii=False, default=str),
                            }
                        )
                        steps.append(
                            {"tool": name, "args": args, "ok": ok, "summary": _summarize(result)}
                        )
                else:
                    # 循环用尽仍未给出最终回复，做一次收尾总结（无工具）
                    reply = reply or "（已达到工具调用上限，部分结果见 steps）"

        return {"reply": reply, "steps": steps, "task": task}
    except Exception as e:  # noqa: BLE001
        return {
            "reply": f"（智能助手处理出错：{e}）",
            "steps": steps,
            "task": task,
        }


def _extract_mcp_result(mcp_res: Any) -> Any:
    """从 MCP CallToolResult 中取出结构化结果（优先解析 JSON 文本）。"""
    content = getattr(mcp_res, "content", None)
    if content:
        first = content[0]
        text = getattr(first, "text", None)
        if text is not None:
            try:
                return json.loads(text)
            except Exception:  # noqa: BLE001
                return text
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

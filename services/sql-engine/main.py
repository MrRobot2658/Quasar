"""
SQL Engine — OLAP 查询层 + 用户分组 + 自然语义查询
Swagger: http://localhost:8002/docs
"""

import os

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse

from agent import NlSegmentAgent
from dsl import DslEngine
from engine import SqlEngine
from etl import EtlService
from executor import create_executor
from groups import GroupService
from nl_query import NlQueryPlanner
from objects import ObjectError, ObjectService
from segments import SegmentService
from tags import TagService
from schemas import (
    DslRule,
    GroupCreate,
    GroupResponse,
    GroupSearchRequest,
    HealthResponse,
    MemberAdd,
    MemberResponse,
    AgentConfirmRequest,
    AgentDraftRequest,
    EtlRequest,
    NlQueryRequest,
    NlQueryResponse,
    ObjectSearchRequest,
    ObjectUpsertRequest,
    QueryRequest,
    QueryResult,
    RelationAddRequest,
    SegmentSaveRequest,
    TagCreate,
    TagResponse,
    TagSearchRequest,
    UserImportRequest,
    UserSummary,
)

# ── 功能模块 Router（00~09）─────────────────────────────────────────────────
from platform_api import router as platform_router
from semantic_api import router as semantic_router
from connections_api import router as connections_router
from unify_api import router as unify_router
from objects_api import router as objects_admin_router
from accounts_api import router as accounts_router
from engage_api import router as engage_router
from protocols_api import router as protocols_router
from privacy_api import router as privacy_router
from monitor_api import router as monitor_router
from settings_api import router as settings_router
from auth_api import router as auth_router
from scheduler_api import router as scheduler_router
from kb_api import router as kb_router
from apps_api import router as apps_router
from analyst_api import router as analyst_router
from feedback_api import router as feedback_router
from feedback import FeedbackService

TAGS = [
    {"name": "系统", "description": "健康检查"},
    {"name": "模板查询", "description": "预置 SQL 模板 + 参数拼装"},
    {"name": "自然语言", "description": "DeepSeek 语义理解 → 模板查询"},
    {"name": "用户分组", "description": "人群包 CRUD 与成员管理"},
    {"name": "用户", "description": "用户画像与分组归属查询"},
    {"name": "用户标签", "description": "多层级标签树与标签筛选"},
    {"name": "多对象", "description": "User/Lead/Account/Product/Store 接入与跨对象筛选"},
    {"name": "DSL", "description": "候选规则校验 / 回显翻译 / 编译 / 人数预估"},
    {"name": "Segment", "description": "人群规则保存"},
    {"name": "NL圈人", "description": "自然语言 → 候选 DSL → 校验/预估 → 确认保存（CDP Agent）"},
]

executor = create_executor()
engine = SqlEngine(executor)
nl_planner = NlQueryPlanner(engine)
group_service = GroupService(executor if hasattr(executor, "config") else None)
tag_service = TagService(executor if hasattr(executor, "config") else None)
object_service = ObjectService(executor if hasattr(executor, "config") else None)
dsl_engine = DslEngine(object_service)
segment_service = SegmentService(executor if hasattr(executor, "config") else None)
nl_agent = NlSegmentAgent(dsl_engine, object_service)
etl_service = EtlService(object_service)
feedback_service = FeedbackService(executor if hasattr(executor, "config") else None)

ROOT_PATH = os.getenv("ROOT_PATH", "")

app = FastAPI(
    title="SQL Engine API",
    description="OLAP 查询层 — 模板 SQL / 自然语言 / 用户分组，与 Doris 解耦",
    version="1.1.0",
    openapi_tags=TAGS,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    root_path=ROOT_PATH,
)


# ── API Key 鉴权（机器调用，如 MCP）─────────────────────────────────────────
# 仅当配置了 CDP_API_KEY 时启用；启用后所有请求须带 X-API-Key 头，否则 401。
# 浏览器经 nginx /api 访问时由网关注入该头；MCP 由 services/mcp/server.py 注入。
# 健康检查 / Swagger 文档放行，便于探活与调试；未配置则完全放行（保持原行为）。
_CDP_API_KEY = os.getenv("CDP_API_KEY", "").strip()
_AUTH_EXEMPT = ("/health", "/docs", "/redoc", "/openapi.json")


@app.middleware("http")
async def _api_key_guard(request, call_next):
    if _CDP_API_KEY and request.method != "OPTIONS":
        path = request.url.path
        if not any(path == p or path.startswith(p + "/") for p in _AUTH_EXEMPT):
            if request.headers.get("x-api-key", "").strip() != _CDP_API_KEY:
                return JSONResponse(status_code=401, content={"detail": "无效或缺失的 API Key"})
    return await call_next(request)


# ── 系统 ──────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["系统"])
def health():
    return {
        "status": "ok",
        "scale_tier": os.getenv("SCALE_TIER", "dev"),
        "doris_be_shards": int(os.getenv("DORIS_BE_SHARDS", "1")),
        "executor": executor.health(),
        "nl_query": {
            "enabled": bool(os.getenv("DEEPSEEK_API_KEY")),
            "model": os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
        },
    }


@app.get("/templates", tags=["模板查询"])
def list_templates():
    return {"templates": engine.list_templates()}


# ── 自然语言 ──────────────────────────────────────────────────────────────

@app.post("/nl-query", response_model=NlQueryResponse, tags=["自然语言"])
def natural_language_query(body: NlQueryRequest):
    """自然语义查询：DeepSeek/规则 → 模板 + 参数 → 执行（支持用户、分组）"""
    try:
        return nl_planner.plan_and_query(body.question, body.tenant_id, body.context)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"DeepSeek API error: {e.response.text}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"NL query failed: {e}")


# ── 模板查询 ──────────────────────────────────────────────────────────────

@app.post("/query/{template_name}", response_model=QueryResult, tags=["模板查询"])
def run_query(template_name: str, body: QueryRequest):
    try:
        return engine.query(template_name, body.params)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OLAP query failed: {e}")


@app.get("/query/{template_name}", response_model=QueryResult, tags=["模板查询"])
def run_query_get(
    template_name: str,
    tenant_id: int,
    one_id: int | None = None,
    phone: str | None = None,
    form_id: str | None = None,
    channel_type: str | None = None,
    channel_id: str | None = None,
    group_id: int | None = None,
    group_code: str | None = None,
    limit: int = 20,
    offset: int = 0,
):
    params: dict = {"tenant_id": tenant_id, "limit": limit, "offset": offset}
    for key, val in [
        ("one_id", one_id), ("phone", phone), ("form_id", form_id),
        ("channel_type", channel_type), ("channel_id", channel_id),
        ("group_id", group_id), ("group_code", group_code),
    ]:
        if val is not None:
            params[key] = val
    try:
        return engine.query(template_name, params)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OLAP query failed: {e}")


# ── 用户分组 ──────────────────────────────────────────────────────────────

@app.post("/groups", response_model=GroupResponse, status_code=201, tags=["用户分组"])
def create_group(body: GroupCreate):
    try:
        return group_service.create_group(body.model_dump())
    except Exception as e:
        if "Duplicate" in str(e):
            raise HTTPException(status_code=409, detail="分组编码已存在")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/groups/{tenant_id}", response_model=list[GroupResponse], tags=["用户分组"])
def list_groups(tenant_id: int):
    return group_service.list_groups(tenant_id)


@app.get("/groups/{tenant_id}/{group_id}", response_model=GroupResponse, tags=["用户分组"])
def get_group(tenant_id: int, group_id: int):
    row = group_service.get_group(tenant_id, group_id)
    if not row:
        raise HTTPException(status_code=404, detail="分组不存在")
    return row


@app.get("/groups/{tenant_id}/code/{group_code}", response_model=GroupResponse, tags=["用户分组"])
def get_group_by_code(tenant_id: int, group_code: str):
    row = group_service.get_group_by_code(tenant_id, group_code)
    if not row:
        raise HTTPException(status_code=404, detail="分组不存在")
    return row


@app.post("/groups/{tenant_id}/{group_id}/members", response_model=MemberResponse, tags=["用户分组"])
def add_member(tenant_id: int, group_id: int, body: MemberAdd):
    if not group_service.get_group(tenant_id, group_id):
        raise HTTPException(status_code=404, detail="分组不存在")
    return group_service.add_member(tenant_id, group_id, body.one_id, body.source)


@app.delete("/groups/{tenant_id}/{group_id}/members/{one_id}", tags=["用户分组"])
def remove_member(tenant_id: int, group_id: int, one_id: int):
    if not group_service.remove_member(tenant_id, group_id, one_id):
        raise HTTPException(status_code=404, detail="成员不存在")
    return {"ok": True}


@app.get("/groups/{tenant_id}/{group_id}/members", tags=["用户分组"])
def list_members(tenant_id: int, group_id: int):
    return group_service.list_members(tenant_id, group_id)


# ── 用户 ──────────────────────────────────────────────────────────────────

@app.get("/users/{tenant_id}/{one_id}", response_model=UserSummary, tags=["用户"])
def get_user(tenant_id: int, one_id: int):
    """用户画像 + 所属分组"""
    row = group_service.get_user_summary(tenant_id, one_id)
    if not row:
        raise HTTPException(status_code=404, detail="用户不存在")
    return row


@app.get("/users/{tenant_id}/{one_id}/groups", tags=["用户"])
def get_user_groups(tenant_id: int, one_id: int):
    return group_service.user_groups(tenant_id, one_id)


@app.post("/groups/search", tags=["用户分组"])
def search_group_members(body: GroupSearchRequest):
    """多分组组合查询成员（AND/OR）"""
    data = group_service.search_members(
        body.tenant_id, body.group_ids, body.operator, body.limit
    )
    return {
        "tenant_id": body.tenant_id,
        "group_ids": body.group_ids,
        "operator": body.operator,
        "row_count": len(data),
        "data": data,
    }


# ── 用户标签 ──────────────────────────────────────────────────────────────

@app.post("/tags", response_model=TagResponse, status_code=201, tags=["用户标签"])
def create_tag(body: TagCreate):
    try:
        return tag_service.create_tag(body.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        if "Duplicate" in str(e):
            raise HTTPException(status_code=409, detail="标签编码已存在")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tags/{tenant_id}", response_model=list[TagResponse], tags=["用户标签"])
def list_tags(tenant_id: int):
    return tag_service.list_tags(tenant_id)


@app.get("/tags/{tenant_id}/tree", response_model=list[TagResponse], tags=["用户标签"])
def get_tag_tree(tenant_id: int):
    return tag_service.get_tree(tenant_id)


@app.get("/tags/{tenant_id}/code/{tag_code}/count", tags=["用户标签"])
def count_users_by_tag(tenant_id: int, tag_code: str):
    return {"tenant_id": tenant_id, "tag_code": tag_code, "count": tag_service.count_by_tag(tenant_id, tag_code)}


@app.post("/users/import", tags=["用户"])
def import_users_proxy(body: UserImportRequest):
    """批量导入用户（代理至 ID-Mapping）"""
    try:
        resp = httpx.post(
            "http://id-mapping:8000/users/import",
            json=body.model_dump(),
            timeout=120.0,
        )
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"ID-Mapping 不可用: {e}")


@app.post("/tags/search", tags=["用户标签"])
def search_users_by_tags(body: TagSearchRequest):
    """多标签组合查询用户（AND/OR）"""
    data = tag_service.search_users(body.tenant_id, body.tag_codes, body.operator, body.limit)
    return {
        "tenant_id": body.tenant_id,
        "tag_codes": body.tag_codes,
        "operator": body.operator,
        "row_count": len(data),
        "data": data,
    }


# ── 多对象（V3.0-06 第 3 章）──────────────────────────────────────────────

@app.get("/objects/meta", tags=["多对象"])
def object_meta():
    """对象类型 + 字段定义 + 关联矩阵"""
    return {"objects": object_service.list_objects(), "relations": object_service.relations()}


@app.post("/objects/upsert", tags=["多对象"])
def upsert_object(body: ObjectUpsertRequest):
    """接入/更新单个对象实体"""
    try:
        return object_service.upsert_object(body.tenant_id, body.object, body.record)
    except ObjectError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/objects/relations", tags=["多对象"])
def add_relation(body: RelationAddRequest):
    """新增对象关联边"""
    try:
        return object_service.add_relation(
            body.tenant_id, body.src_type, body.src_id, body.rel_type, body.dst_type, body.dst_id
        )
    except ObjectError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/objects/search", tags=["多对象"])
def search_objects(body: ObjectSearchRequest):
    """跨对象筛选（base 条件 + relation 条件，经 object_relations JOIN，≤3 跳）"""
    try:
        return object_service.search(
            body.tenant_id, body.object,
            [c.model_dump() for c in body.conditions],
            [r.model_dump() for r in body.relations],
            body.limit, body.count_only,
        )
    except ObjectError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"对象筛选失败: {e}")


# ── 可视化 ETL（多源 → 字段映射 → 导入多对象 → 进筛选）──────────────────────

@app.post("/etl/preview", tags=["ETL"])
def etl_preview(body: EtlRequest):
    """干跑：解析源 + 映射前 N 行，返回样例记录与校验问题，不写库。"""
    try:
        return etl_service.preview(
            body.tenant_id, body.target_object, body.source.model_dump(),
            [m.model_dump() for m in body.mapping], body.limit_preview)
    except ObjectError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/etl/import", tags=["ETL"])
def etl_import(body: EtlRequest):
    """执行导入：逐行 upsert 到目标对象（可选建关系），错误按行返回。"""
    try:
        return etl_service.run_import(
            body.tenant_id, body.target_object, body.source.model_dump(),
            [m.model_dump() for m in body.mapping],
            body.link.model_dump() if body.link else None,
            govern=body.govern)
    except ObjectError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── 元数据（Filter Engine 代理）──────────────────────────────────────────

@app.get("/metadata/{tenant_id}/fields", tags=["DSL"])
def metadata_fields(tenant_id: int, object: str | None = None):
    """字段树 + 操作符 + 关联矩阵（tenant_id 预留权限过滤）"""
    objs = object_service.list_objects()
    if object:
        objs = [o for o in objs if o["object"] == object]
        if not objs:
            raise HTTPException(status_code=404, detail=f"未知对象: {object}")
    return {"tenant_id": tenant_id, "objects": objs, "relations": object_service.relations()}


# ── DSL 验证层（V3.0-06 Ch4.1–4.3）────────────────────────────────────────

@app.post("/dsl/validate", tags=["DSL"])
def dsl_validate(body: DslRule):
    """结构 + 字段/操作符/关联/跳数 校验（不执行）"""
    return dsl_engine.validate(body.model_dump())


@app.post("/dsl/echo", tags=["DSL"])
def dsl_echo(body: DslRule):
    """规则回显：业务可读摘要 + 每条件解释（字段/操作符/取值）"""
    try:
        return dsl_engine.echo(body.model_dump())
    except ObjectError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/dsl/compile", tags=["DSL"])
def dsl_compile(body: DslRule, count_only: bool = False):
    """DSL → SQL（不执行，返回 SQL + 参数）"""
    try:
        return dsl_engine.compile(body.model_dump(), count_only=count_only)
    except ObjectError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/dsl/estimate", tags=["DSL"])
def dsl_estimate(body: DslRule):
    """dry-run 人数预估（COUNT）"""
    try:
        return dsl_engine.estimate(body.model_dump())
    except ObjectError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── Segment 保存 ──────────────────────────────────────────────────────────

@app.post("/segments", status_code=201, tags=["Segment"])
def save_segment(body: SegmentSaveRequest):
    """保存候选规则为 Segment（先校验，再计算人数预估）"""
    val = dsl_engine.validate(body.dsl | {"tenant_id": body.tenant_id})
    if not val["ok"]:
        raise HTTPException(status_code=400, detail={"msg": "DSL 校验失败", "errors": val["errors"]})
    est = dsl_engine.estimate(body.dsl | {"tenant_id": body.tenant_id})
    try:
        saved = segment_service.save(body.tenant_id, body.segment_code, body.segment_name,
                                     body.dsl, est["estimate"], body.source)
        # 反馈埋点（不阻塞保存）：标签圈选频次 + 画像字段消费
        try:
            conds = body.dsl.get("conditions", []) or []
            tag_codes = [v for c in conds if c.get("field") == "tags"
                         for v in (c["value"] if isinstance(c.get("value"), list) else [c.get("value")]) if v]
            if tag_codes:
                feedback_service.log_tag_usage(body.tenant_id, body.segment_code, [str(t) for t in tag_codes])
            fields = [c.get("field") for c in conds if c.get("field")]
            if fields:
                feedback_service.record_field_access(body.tenant_id, body.dsl.get("object", "user"), fields)
        except Exception:  # noqa: BLE001
            pass
        return saved
    except Exception as e:
        if "Duplicate" in str(e):
            raise HTTPException(status_code=409, detail="Segment 编码已存在")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/segments/{tenant_id}", tags=["Segment"])
def list_segments(tenant_id: int):
    return segment_service.list(tenant_id)


@app.get("/segments/{tenant_id}/{segment_code}", tags=["Segment"])
def get_segment(tenant_id: int, segment_code: str):
    row = segment_service.get(tenant_id, segment_code)
    if not row:
        raise HTTPException(status_code=404, detail="Segment 不存在")
    return row


# ── NL 圈人（Ch4 / CDP Agent Phase 1）─────────────────────────────────────

@app.post("/agent/segment/draft", tags=["NL圈人"])
def agent_draft(body: AgentDraftRequest):
    """自然语言 → 候选 DSL → 校验 + 回显 + 人数预估（或返回澄清问题）"""
    try:
        return nl_agent.draft(body.question, body.tenant_id, body.context)
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"DeepSeek API error: {e.response.text}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"NL 圈人失败: {e}")


@app.post("/agent/segment/confirm", status_code=201, tags=["NL圈人"])
def agent_confirm(body: AgentConfirmRequest):
    """用户确认候选规则 → 走保存链路（source=nl-agent）"""
    try:
        saved = nl_agent.confirm(body.tenant_id, body.segment_code, body.segment_name,
                                 body.rule, segment_service)
        try:
            rule = body.rule if isinstance(body.rule, dict) else {}
            conds = rule.get("conditions", []) or []
            tag_codes = [v for c in conds if c.get("field") == "tags"
                         for v in (c["value"] if isinstance(c.get("value"), list) else [c.get("value")]) if v]
            if tag_codes:
                feedback_service.log_tag_usage(body.tenant_id, body.segment_code, [str(t) for t in tag_codes])
            fields = [c.get("field") for c in conds if c.get("field")]
            if fields:
                feedback_service.record_field_access(body.tenant_id, rule.get("object", "user"), fields)
        except Exception:  # noqa: BLE001
            pass
        return saved
    except ObjectError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        if "Duplicate" in str(e):
            raise HTTPException(status_code=409, detail="Segment 编码已存在")
        raise HTTPException(status_code=500, detail=str(e))


# ── 挂载功能模块 Router（在内联端点之后注册，内联端点保持优先）────────────────
app.include_router(platform_router)
app.include_router(semantic_router)
app.include_router(connections_router)
app.include_router(unify_router)
app.include_router(objects_admin_router)
app.include_router(accounts_router)
app.include_router(engage_router)
app.include_router(protocols_router)
app.include_router(privacy_router)
app.include_router(monitor_router)
app.include_router(settings_router)
app.include_router(auth_router)
app.include_router(scheduler_router)
app.include_router(kb_router)
app.include_router(apps_router)
app.include_router(analyst_router)
app.include_router(feedback_router)


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
        tags=TAGS,
    )
    schema["info"]["x-services"] = {
        "id-mapping": "http://localhost:8001/openapi.json",
        "sql-engine": "http://localhost:8002/openapi.json",
    }
    if ROOT_PATH:
        schema["servers"] = [{"url": ROOT_PATH}]
    else:
        schema["servers"] = [{"url": "/"}]
    app.openapi_schema = schema
    return app.openapi_schema


app.openapi = custom_openapi


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

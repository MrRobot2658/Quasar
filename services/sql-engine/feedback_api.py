"""反馈闭环路由（/feedback/*）。详见 docs/13-feedback-loop.md。

GET 只读查询；POST 触发幂等聚合/回写或记录回传。所有能力委托给 FeedbackService，
本层只做参数透传与租户隔离（tenant_id 必填）。
"""
from __future__ import annotations

from fastapi import APIRouter, Body, Query

from feedback import FeedbackService

router = APIRouter(prefix="/feedback", tags=["反馈闭环"])
_svc = FeedbackService()


# ── Skill 1 · 客户360 · 画像字段健康 ───────────────────────────────────────
@router.post("/field/access")
def field_access(tenant_id: int = Query(...), object_type: str = Query(...),
                 fields: list[str] = Body(..., embed=True)):
    return _svc.record_field_access(tenant_id, object_type, fields)


@router.post("/field/scan")
def field_scan(tenant_id: int = Query(...), object_type: str = Query("user")):
    return {"object": object_type, "fields": _svc.scan_field_health(tenant_id, object_type)}


@router.get("/field/health")
def field_health(tenant_id: int = Query(...), object_type: str = Query("user")):
    return {"object": object_type, "fields": _svc.field_health(tenant_id, object_type)}


# ── Skill 2 · 标签体系 · 加权/归档 ─────────────────────────────────────────
@router.post("/tag/usage")
def tag_usage(tenant_id: int = Query(...), segment_code: str = Query(""),
              tag_codes: list[str] = Body(..., embed=True)):
    return _svc.log_tag_usage(tenant_id, segment_code, tag_codes)


@router.post("/tag/aggregate")
def tag_aggregate(tenant_id: int = Query(...)):
    return _svc.aggregate_tag_feedback(tenant_id)


@router.get("/tag/health")
def tag_health(tenant_id: int = Query(...)):
    return {"tags": _svc.tag_health(tenant_id)}


# ── Skill 3 · 人群圈选 · 触达回写 + 条件建议 ───────────────────────────────
@router.post("/segment/aggregate")
def segment_aggregate(tenant_id: int = Query(...)):
    return _svc.aggregate_segment_feedback(tenant_id)


@router.get("/segment/quality")
def segment_quality(tenant_id: int = Query(...)):
    return {"segments": _svc.segment_quality(tenant_id)}


@router.get("/segment/suggest")
def segment_suggest(tenant_id: int = Query(...), segment_code: str = Query(...)):
    return _svc.suggest_segment_conditions(tenant_id, segment_code)


# ── Skill 4 · 数据治理 · 质量巡检 + 质量分 ─────────────────────────────────
@router.post("/quality/scan")
def quality_scan(tenant_id: int = Query(...), object_type: str = Query("user")):
    return _svc.scan_data_quality(tenant_id, object_type)


@router.get("/quality/report")
def quality_report(tenant_id: int = Query(...)):
    return _svc.quality_report(tenant_id)


# ── Skill 5 · 分析洞察 · 异常发现 + 决策回传 ───────────────────────────────
@router.post("/insight/detect")
def insight_detect(tenant_id: int = Query(...)):
    return {"findings": _svc.detect_insights(tenant_id)}


@router.get("/insight/findings")
def insight_findings(tenant_id: int = Query(...), status: str | None = Query(None)):
    return {"findings": _svc.list_findings(tenant_id, status)}


@router.post("/insight/decision")
def insight_decision(tenant_id: int = Query(...), finding_id: int = Query(...),
                     decision: str = Body(..., embed=True),
                     result: str | None = Body(None, embed=True),
                     status: str = Body("acted", embed=True)):
    return _svc.record_decision(tenant_id, finding_id, decision, result, status)


# ── 编排 · 一次跑完全部聚合（DAG / 手动刷新）─────────────────────────────────
@router.post("/run-all")
def run_all(tenant_id: int = Query(...)):
    return _svc.run_all(tenant_id)

"""反馈闭环回写 DAG（feedback_loop）。详见 docs/13-feedback-loop.md。

每小时把下游的触达/消费/质量信号聚合回写给上游资产（segments / tag_definitions /
field_usage / data_quality_checks / insight_findings）。每个 Skill 一个 task，全部委托
给 sql-engine 的 /feedback/* 端点——回写逻辑只在服务层一处实现，DAG 仅做编排与遍历租户。

优雅降级：用 stdlib urllib（不引第三方依赖）；任一租户/端点失败只记日志不让整条链路失败，
sql-engine 不可达时直接跳过（前端「立即刷新」按钮仍可手动触发同一端点）。
触发方式：Airflow 调度（@hourly），或经 REST API 手动触发并在 conf 里传 tenant_ids 覆盖。
"""
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

from airflow.decorators import dag, task

SQL_ENGINE_URL = os.getenv("SQL_ENGINE_URL", "http://sql-engine:8000")
DEFAULT_TENANTS = [1001, 1002]


def _post(path: str, tenant_id: int, **params) -> dict:
    qs = urllib.parse.urlencode({"tenant_id": tenant_id, **params})
    url = f"{SQL_ENGINE_URL}{path}?{qs}"
    req = urllib.request.Request(url, method="POST", data=b"", headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8") or "{}")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError) as e:
        print(f"[feedback_loop] {path} tenant={tenant_id} 跳过：{e}")
        return {"skipped": str(e)}


def _tenants(context) -> list:
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    return conf.get("tenant_ids") or DEFAULT_TENANTS


@dag(
    dag_id="feedback_loop",
    description="CDP 五大 Skill 反馈闭环回写（触达/消费/质量 → 资产自迭代）",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["dataagent", "feedback"],
)
def feedback_loop():
    @task
    def aggregate_segments(**context) -> dict:
        """Skill 3：触达回执 → segments.conversion_rate/quality_score。"""
        return {t: _post("/feedback/segment/aggregate", t) for t in _tenants(context)}

    @task
    def aggregate_tags(**context) -> dict:
        """Skill 2：圈选频次 + 转化率 → tag_definitions.weight/status（依赖 segment 已回写）。"""
        return {t: _post("/feedback/tag/aggregate", t) for t in _tenants(context)}

    @task
    def scan_field_health(**context) -> dict:
        """Skill 1：画像字段填充率 + 消费频次 → field_usage.recommendation。"""
        out = {}
        for t in _tenants(context):
            out[t] = {obj: _post("/feedback/field/scan", t, object_type=obj)
                      for obj in ("user", "lead", "account")}
        return out

    @task
    def scan_quality(**context) -> dict:
        """Skill 4：空值/重复/格式巡检 → data_quality_checks + violations。"""
        out = {}
        for t in _tenants(context):
            out[t] = {obj: _post("/feedback/quality/scan", t, object_type=obj)
                      for obj in ("user", "lead", "account", "order")}
        return out

    @task
    def detect_insights(**context) -> dict:
        """Skill 5：SKU 断崖 / 客群漂移 → insight_findings。"""
        return {t: _post("/feedback/insight/detect", t) for t in _tenants(context)}

    # Skill 3 先回写 segment，Skill 2 标签聚合依赖它；其余可并行
    seg = aggregate_segments()
    seg >> aggregate_tags()
    scan_field_health()
    scan_quality()
    detect_insights()


feedback_loop()

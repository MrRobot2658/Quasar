"""Apache Airflow 对接：把可视化编排 Pipelines 推送/运行到真实调度器。

- Airflow（scheduler + webserver）跑在 compose 的 airflow 服务，REST API v2 在 /api/v1。
- 鉴权：basic auth（admin/admin）。
- 模型：不为每个 pipeline 建 DAG，而是用一个参数化 DAG `agenticdatahub_pipeline` 承载所有运行，
  触发时把 pipeline 信息放进 dag_run.conf。触发 DAG run 是 Airflow 稳定支持的 API，简单可靠。
- 路由前缀 /connections（与 connections_api 同前缀），经 nginx 暴露为 /api/connections/scheduler/*。
"""
from __future__ import annotations

import os
import time

import httpx
from fastapi import APIRouter

AIRFLOW_API = os.getenv("AIRFLOW_API_URL", "http://airflow:8080/api/v1").rstrip("/")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
AIRFLOW_DAG_ID = os.getenv("AIRFLOW_DAG_ID", "agenticdatahub_pipeline")
# 浏览器可达的 Airflow UI（容器内主机名 airflow 浏览器访问不到，故单独配置 localhost）
AIRFLOW_UI = os.getenv("AIRFLOW_UI_URL", "http://localhost:8088")


class AfError(Exception):
    pass


class AirflowClient:
    def _client(self) -> httpx.Client:
        return httpx.Client(timeout=20, trust_env=False, auth=(AIRFLOW_USER, AIRFLOW_PASSWORD))

    def health(self) -> dict:
        try:
            # /health 无需鉴权；再查 DAG 是否已被 Airflow 解析到
            with httpx.Client(timeout=10, trust_env=False) as c:
                h = c.get(f"{AIRFLOW_API}/health").json()
            dag = None
            with self._client() as c:
                r = c.get(f"{AIRFLOW_API}/dags/{AIRFLOW_DAG_ID}")
                if r.status_code == 200:
                    j = r.json()
                    dag = {"dag_id": j.get("dag_id"), "is_paused": j.get("is_paused")}
            sched = (h.get("scheduler") or {}).get("status")
            meta = (h.get("metadatabase") or {}).get("status")
            return {"reachable": True, "scheduler": sched, "metadatabase": meta,
                    "dag": dag, "ui_url": AIRFLOW_UI, "dag_id": AIRFLOW_DAG_ID}
        except Exception as e:  # noqa: BLE001
            return {"reachable": False, "error": str(e), "ui_url": AIRFLOW_UI, "dag_id": AIRFLOW_DAG_ID}

    def ensure_unpaused(self, dag_id: str) -> None:
        with self._client() as c:
            c.patch(f"{AIRFLOW_API}/dags/{dag_id}", json={"is_paused": False})

    def trigger(self, dag_id: str, conf: dict, run_id: str) -> dict:
        with self._client() as c:
            r = c.post(f"{AIRFLOW_API}/dags/{dag_id}/dagRuns",
                       json={"dag_run_id": run_id, "conf": conf})
        if r.status_code not in (200, 409):
            raise AfError(f"触发失败（{r.status_code}）：{r.text[:200]}")
        j = r.json()
        return {"dag_run_id": j.get("dag_run_id", run_id), "state": j.get("state")}

    def last_runs(self, dag_id: str, limit: int = 5) -> list:
        with self._client() as c:
            r = c.get(f"{AIRFLOW_API}/dags/{dag_id}/dagRuns",
                      params={"order_by": "-execution_date", "limit": limit})
        return (r.json().get("dag_runs") or []) if r.status_code == 200 else []


_client = AirflowClient()
router = APIRouter(prefix="/connections", tags=["scheduler"])


@router.get("/scheduler/health")
def scheduler_health():
    return _client.health()


def deploy_and_run(tenant_id: int, pipeline_name: str, nodes: list, edges: list, run: bool = True) -> dict:
    """供 connections_api 的 execute_pipeline 调用：触发 Airflow DAG 运行该 pipeline。"""
    out: dict = {"engine": "airflow", "reachable": True, "ui_url": AIRFLOW_UI, "dag_id": AIRFLOW_DAG_ID}
    # 把节点列表（精简后）放进 conf，DAG 据此动态展开为多任务；上限 100 个，控制 conf 体积
    conf_nodes = [{
        "id": n.get("id"), "label": n.get("label"),
        "type": n.get("type"), "kind": n.get("kind"),
    } for n in (nodes or [])][:100]
    conf = {
        "tenant_id": tenant_id, "pipeline_name": pipeline_name,
        "nodes": conf_nodes, "node_count": len(nodes or []), "edge_count": len(edges or []),
    }
    try:
        self_unpause = _client.ensure_unpaused
        self_unpause(AIRFLOW_DAG_ID)  # 幂等，确保 DAG 启用
    except Exception:  # noqa: BLE001
        pass
    if run:
        run_id = f"adh__{tenant_id}__{int(time.time() * 1000)}"
        out["dag_run"] = _client.trigger(AIRFLOW_DAG_ID, conf, run_id)
        out["ui_url"] = f"{AIRFLOW_UI}/dags/{AIRFLOW_DAG_ID}/grid"
    return out

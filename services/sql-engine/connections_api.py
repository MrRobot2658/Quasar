"""01 · connections 模块 — 数据源 / 目的地 / Reverse-ETL / 数据仓库 / 函数 / 管道

只做加法：本文件自带 APIRouter（变量名 router）、ConnectionsService 与 Pydantic 模型，
不修改 main.py / schemas.py / 既有 service。所有 SQL 参数化，所有操作按 tenant_id 隔离。
对标 Twilio Segment 的 Connections（Sources/Destinations/Reverse ETL/Warehouses/Functions）。
"""

import json
import secrets
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import pymysql
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from executor import MysqlOlapExecutor
from objects import ObjectError, ObjectService


# ════════════════════════════════════════════════════════════════════════
# Pydantic 模型（本文件内，不写进 schemas.py）
# ════════════════════════════════════════════════════════════════════════

class SourceCreate(BaseModel):
    source_name: str
    source_type: str = Field(description="连接器类型：csv/api/javascript/kafka · mysql/postgres/sqlserver/oracle/clickhouse/mongodb/elasticsearch · snowflake/bigquery/redshift · iceberg/delta/s3")
    config: dict[str, Any] = Field(default_factory=dict)
    schema_def: dict[str, Any] | None = Field(default=None, alias="schema")

    model_config = {"populate_by_name": True}


class TestConnectionRequest(BaseModel):
    config: dict[str, Any] = Field(default_factory=dict)


class TrackEvent(BaseModel):
    event_type: str
    data: dict[str, Any] = Field(default_factory=dict)
    anonymousId: str | None = None
    userId: str | None = None
    timestamp: str | None = None


class TrackRequest(BaseModel):
    write_key: str
    events: list[TrackEvent] = Field(default_factory=list)


class DestinationCreate(BaseModel):
    destination_name: str
    destination_type: str = Field(description="ads/marketing/bi/webhook")
    config: dict[str, Any] = Field(default_factory=dict)


class DestinationTestRequest(BaseModel):
    sample_data: dict[str, Any] = Field(default_factory=dict)


class MappingItem(BaseModel):
    source_field: str
    target_field: str
    transform_logic: dict[str, Any] | None = None


class DestinationMappingRequest(BaseModel):
    source_object: str
    mapping: list[MappingItem] = Field(default_factory=list)


class ReverseEtlJobCreate(BaseModel):
    job_name: str
    source_object: str
    destination_id: str
    schedule_cron: str = "0 */15 * * * *"
    enabled: bool = True


class WarehouseCreate(BaseModel):
    warehouse_name: str
    warehouse_type: str = Field(description="数仓/库类型：doris/hive/mysql/postgres/clickhouse · snowflake/bigquery/redshift · iceberg/delta")
    connection_string: str = ""
    username: str = ""
    password: str = ""
    database_name: str = ""
    sync_frequency_seconds: int | None = None


class FunctionCreate(BaseModel):
    function_name: str
    function_type: str = Field(description="source_function/destination_function")
    language: str = "javascript"
    code: str = ""
    entry_point: str = "onEvent"


class PipelineCreate(BaseModel):
    pipeline_name: str
    nodes: list[dict[str, Any]] = Field(default_factory=list)
    edges: list[dict[str, Any]] = Field(default_factory=list)
    status: str = "draft"


class PipelineExecuteRequest(BaseModel):
    source_config: dict[str, Any] | None = None
    destination_config: dict[str, Any] | None = None


# ════════════════════════════════════════════════════════════════════════
# Service — 仿 groups.py / tags.py / segments.py 风格，复用 MysqlOlapExecutor 取连接
# ════════════════════════════════════════════════════════════════════════

_JSON_FIELDS = (
    "config", "schema_def", "data", "transform_logic", "tables_synced",
    "nodes", "edges", "input", "output",
)


class ConnectionsService:
    def __init__(self, executor: MysqlOlapExecutor | None = None):
        self._executor = executor or MysqlOlapExecutor()
        self.config = self._executor.config
        self._objects = ObjectService(self._executor)

    @contextmanager
    def _conn(self):
        conn = pymysql.connect(**self.config, autocommit=True)
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _nid(prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:16]}"

    @staticmethod
    def _mask(write_key: str | None) -> str | None:
        if not write_key:
            return write_key
        if len(write_key) <= 8:
            return "****"
        return f"{write_key[:4]}{'*' * 8}{write_key[-4:]}"

    def _normalize(self, row: dict | None) -> dict | None:
        if not row:
            return None
        for k in _JSON_FIELDS:
            if k in row and isinstance(row[k], str):
                try:
                    row[k] = json.loads(row[k])
                except json.JSONDecodeError:
                    pass
        # schema_def 对外暴露为 schema
        if "schema_def" in row:
            row["schema"] = row.pop("schema_def")
        return row

    # ── Sources ──────────────────────────────────────────────────────────
    def list_sources(self, tenant_id: int) -> list[dict]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT source_id, source_name, source_type, write_key, status,
                       last_event_time, event_count_24h
                FROM connections_sources WHERE tenant_id=%s ORDER BY created_at DESC
                """,
                (tenant_id,),
            )
            rows = cur.fetchall()
        for r in rows:
            r["write_key"] = self._mask(r.get("write_key"))
        return rows

    def create_source(self, tenant_id: int, data: SourceCreate) -> dict:
        source_id = self._nid("src")
        write_key = secrets.token_urlsafe(24)
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO connections_sources
                    (tenant_id, source_id, source_name, source_type, write_key, config, schema_def, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'active')
                """,
                (
                    tenant_id, source_id, data.source_name, data.source_type, write_key,
                    json.dumps(data.config or {}, ensure_ascii=False),
                    json.dumps(data.schema_def or {}, ensure_ascii=False),
                ),
            )
        return {"source_id": source_id, "write_key": write_key, "source_name": data.source_name}

    def get_source(self, tenant_id: int, source_id: str) -> dict | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM connections_sources WHERE tenant_id=%s AND source_id=%s",
                (tenant_id, source_id),
            )
            row = self._normalize(cur.fetchone())
        if not row:
            return None
        row["recent_events"] = self.list_source_events(tenant_id, source_id, 20)
        return row

    def test_source(self, tenant_id: int, source_id: str, config: dict) -> dict:
        """模拟连接测试：源存在即返回样例行（开发环境不实际外连）。"""
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT source_type FROM connections_sources WHERE tenant_id=%s AND source_id=%s",
                (tenant_id, source_id),
            )
            row = cur.fetchone()
        if not row:
            return {"ok": False, "error": "数据源不存在"}
        return {
            "ok": True,
            "sample_rows": [
                {"col": "demo", "value": 1, "source_type": row["source_type"]},
            ],
        }

    def list_source_events(self, tenant_id: int, source_id: str, limit: int = 50) -> list[dict]:
        limit = min(max(limit, 1), 500)
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, event_type, event_timestamp, anonymous_id, user_id,
                       status, error_msg, created_at
                FROM connections_source_events
                WHERE tenant_id=%s AND source_id=%s
                ORDER BY created_at DESC LIMIT %s
                """,
                (tenant_id, source_id, limit),
            )
            rows = cur.fetchall()
        for r in rows:
            r["anonymousId"] = r.pop("anonymous_id", None)
            r["timestamp"] = r.pop("event_timestamp", None)
        return rows

    def track(self, tenant_id: int, body: TrackRequest) -> dict:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT source_id FROM connections_sources WHERE tenant_id=%s AND write_key=%s",
                (tenant_id, body.write_key),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=401, detail="write_key 无效")
            source_id = row["source_id"]
            queued = 0
            for ev in body.events:
                event_id = uuid.uuid4().hex
                ts = ev.timestamp or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                cur.execute(
                    """
                    INSERT INTO connections_source_events
                        (tenant_id, source_id, event_id, event_type, event_timestamp,
                         anonymous_id, user_id, data, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'success')
                    """,
                    (
                        tenant_id, source_id, event_id, ev.event_type, ts,
                        ev.anonymousId, ev.userId,
                        json.dumps(ev.data or {}, ensure_ascii=False),
                    ),
                )
                queued += 1
            cur.execute(
                """
                UPDATE connections_sources
                SET last_event_time=NOW(), event_count_24h=event_count_24h+%s
                WHERE tenant_id=%s AND source_id=%s
                """,
                (queued, tenant_id, source_id),
            )
        return {"ok": True, "queued": queued}

    # ── Destinations ─────────────────────────────────────────────────────
    def list_destinations(self, tenant_id: int) -> list[dict]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT destination_id, destination_name, destination_type, enabled
                FROM connections_destinations WHERE tenant_id=%s ORDER BY created_at DESC
                """,
                (tenant_id,),
            )
            return cur.fetchall()

    def create_destination(self, tenant_id: int, data: DestinationCreate) -> dict:
        destination_id = self._nid("dst")
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO connections_destinations
                    (tenant_id, destination_id, destination_name, destination_type, config, enabled)
                VALUES (%s, %s, %s, %s, %s, 1)
                """,
                (
                    tenant_id, destination_id, data.destination_name, data.destination_type,
                    json.dumps(data.config or {}, ensure_ascii=False),
                ),
            )
        return {"destination_id": destination_id, "destination_name": data.destination_name}

    def get_destination(self, tenant_id: int, destination_id: str) -> dict | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM connections_destinations WHERE tenant_id=%s AND destination_id=%s",
                (tenant_id, destination_id),
            )
            row = self._normalize(cur.fetchone())
            if not row:
                return None
            cur.execute(
                """
                SELECT source_field, target_field, transform_logic, source_object
                FROM connections_destination_mappings
                WHERE tenant_id=%s AND destination_id=%s
                """,
                (tenant_id, destination_id),
            )
            mappings = [self._normalize(m) for m in cur.fetchall()]
        row["mappings"] = mappings
        row["status"] = "enabled" if row.get("enabled") else "disabled"
        return row

    def test_destination(self, tenant_id: int, destination_id: str, sample_data: dict) -> dict:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT destination_id FROM connections_destinations WHERE tenant_id=%s AND destination_id=%s",
                (tenant_id, destination_id),
            )
            row = cur.fetchone()
        if not row:
            return {"ok": False, "latency_ms": 0, "error": "目的地不存在"}
        return {"ok": True, "latency_ms": 42}

    def save_mappings(self, tenant_id: int, destination_id: str, body: DestinationMappingRequest) -> dict:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT destination_id FROM connections_destinations WHERE tenant_id=%s AND destination_id=%s",
                (tenant_id, destination_id),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="目的地不存在")
            # 覆盖式保存该 source_object 的映射
            cur.execute(
                """
                DELETE FROM connections_destination_mappings
                WHERE tenant_id=%s AND destination_id=%s AND source_object=%s
                """,
                (tenant_id, destination_id, body.source_object),
            )
            for m in body.mapping:
                cur.execute(
                    """
                    INSERT INTO connections_destination_mappings
                        (tenant_id, mapping_id, destination_id, source_object,
                         target_field, source_field, transform_logic)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        tenant_id, self._nid("map"), destination_id, body.source_object,
                        m.target_field, m.source_field,
                        json.dumps(m.transform_logic, ensure_ascii=False) if m.transform_logic else None,
                    ),
                )
        return {"ok": True}

    # ── Reverse-ETL ──────────────────────────────────────────────────────
    def list_reverse_etl_jobs(self, tenant_id: int) -> list[dict]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT job_id, job_name, source_object, destination_id, schedule_cron,
                       enabled, next_run_time, last_status
                FROM connections_reverse_etl_jobs WHERE tenant_id=%s ORDER BY created_at DESC
                """,
                (tenant_id,),
            )
            return cur.fetchall()

    def create_reverse_etl_job(self, tenant_id: int, data: ReverseEtlJobCreate) -> dict:
        job_id = self._nid("retl")
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO connections_reverse_etl_jobs
                    (tenant_id, job_id, job_name, source_object, destination_id,
                     schedule_cron, enabled, last_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
                """,
                (
                    tenant_id, job_id, data.job_name, data.source_object, data.destination_id,
                    data.schedule_cron, 1 if data.enabled else 0,
                ),
            )
        return {"job_id": job_id, "job_name": data.job_name}

    def list_reverse_etl_runs(self, tenant_id: int, job_id: str, limit: int = 50) -> list[dict]:
        limit = min(max(limit, 1), 200)
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_id, start_time, duration_ms, row_count, status, error_msg
                FROM connections_reverse_etl_runs
                WHERE tenant_id=%s AND job_id=%s ORDER BY created_at DESC LIMIT %s
                """,
                (tenant_id, job_id, limit),
            )
            return cur.fetchall()

    def run_reverse_etl_now(self, tenant_id: int, job_id: str) -> dict:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT source_object FROM connections_reverse_etl_jobs WHERE tenant_id=%s AND job_id=%s",
                (tenant_id, job_id),
            )
            job = cur.fetchone()
            if not job:
                raise HTTPException(status_code=404, detail="任务不存在")
            # 复用 ObjectService 统计源对象规模（参数化，绝不手拼 SQL）；非注册对象则规模置 0
            row_count = self._safe_estimate(tenant_id, job["source_object"])
            run_id = self._nid("run")
            cur.execute(
                """
                INSERT INTO connections_reverse_etl_runs
                    (tenant_id, run_id, job_id, start_time, row_count, status)
                VALUES (%s, %s, %s, NOW(), %s, 'pending')
                """,
                (tenant_id, run_id, job_id, row_count),
            )
            cur.execute(
                """
                UPDATE connections_reverse_etl_jobs
                SET last_run_time=NOW(), last_status='pending' WHERE tenant_id=%s AND job_id=%s
                """,
                (tenant_id, job_id),
            )
        return {"run_id": run_id, "status": "pending"}

    def complete_reverse_etl_run(self, tenant_id: int, run_id: str,
                                 status: str = "success", error_msg: str | None = None) -> dict:
        """收尾一次 reverse-ETL 运行（pending → success/failed）。调度模拟的「后台跑完」回调。
        duration_ms 取 start_time 到此刻的实际间隔；同步刷新任务的 last_status。"""
        status = status if status in ("success", "failed") else "success"
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT job_id, start_time FROM connections_reverse_etl_runs "
                "WHERE tenant_id=%s AND run_id=%s",
                (tenant_id, run_id),
            )
            run = cur.fetchone()
            if not run:
                raise HTTPException(status_code=404, detail="运行记录不存在")
            cur.execute(
                """
                UPDATE connections_reverse_etl_runs
                SET status=%s, end_time=NOW(),
                    duration_ms=TIMESTAMPDIFF(MICROSECOND, start_time, NOW()) DIV 1000,
                    error_msg=%s
                WHERE tenant_id=%s AND run_id=%s
                """,
                (status, error_msg, tenant_id, run_id),
            )
            cur.execute(
                "UPDATE connections_reverse_etl_jobs SET last_status=%s "
                "WHERE tenant_id=%s AND job_id=%s",
                (status, tenant_id, run["job_id"]),
            )
        return {"run_id": run_id, "status": status}

    def _safe_estimate(self, tenant_id: int, source_object: str) -> int:
        try:
            res = self._objects.search(tenant_id, source_object, None, None, count_only=True)
            return int(res.get("estimate") or 0)
        except (ObjectError, Exception):
            return 0

    # ── Warehouses ───────────────────────────────────────────────────────
    def list_warehouses(self, tenant_id: int) -> list[dict]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT warehouse_id, warehouse_name, warehouse_type, status,
                       last_sync_time, tables_synced
                FROM connections_warehouses WHERE tenant_id=%s ORDER BY created_at DESC
                """,
                (tenant_id,),
            )
            rows = cur.fetchall()
        return [self._normalize(r) for r in rows]

    def create_warehouse(self, tenant_id: int, data: WarehouseCreate) -> dict:
        warehouse_id = self._nid("wh")
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO connections_warehouses
                    (tenant_id, warehouse_id, warehouse_name, warehouse_type,
                     connection_string, username, password, database_name,
                     sync_frequency_seconds, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'testing')
                """,
                (
                    tenant_id, warehouse_id, data.warehouse_name, data.warehouse_type,
                    data.connection_string, data.username, data.password, data.database_name,
                    data.sync_frequency_seconds,
                ),
            )
        return {"warehouse_id": warehouse_id, "warehouse_name": data.warehouse_name, "status": "testing"}

    def sync_warehouse(self, tenant_id: int, warehouse_id: str) -> dict:
        queued = ["doris_user_wide", "object_order", "object_account"]
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT warehouse_id FROM connections_warehouses WHERE tenant_id=%s AND warehouse_id=%s",
                (tenant_id, warehouse_id),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="数据仓库不存在")
            cur.execute(
                """
                UPDATE connections_warehouses
                SET last_sync_time=NOW(), status='healthy', tables_synced=%s
                WHERE tenant_id=%s AND warehouse_id=%s
                """,
                (json.dumps(queued, ensure_ascii=False), tenant_id, warehouse_id),
            )
        return {"ok": True, "queued_tables": queued}

    # ── Functions ────────────────────────────────────────────────────────
    def list_functions(self, tenant_id: int) -> list[dict]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT f.function_id, f.function_name, f.function_type, f.language, f.status,
                       COALESCE(s.runs_7d, 0) AS runs_7d, COALESCE(s.errors_7d, 0) AS errors_7d
                FROM connections_functions f
                LEFT JOIN (
                    SELECT function_id,
                           COUNT(*) AS runs_7d,
                           SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS errors_7d
                    FROM connections_function_runs
                    WHERE tenant_id=%s AND created_at >= NOW() - INTERVAL 7 DAY
                    GROUP BY function_id
                ) s ON s.function_id = f.function_id
                WHERE f.tenant_id=%s ORDER BY f.created_at DESC
                """,
                (tenant_id, tenant_id),
            )
            return cur.fetchall()

    def create_function(self, tenant_id: int, data: FunctionCreate) -> dict:
        function_id = self._nid("fn")
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO connections_functions
                    (tenant_id, function_id, function_name, function_type, language,
                     code, entry_point, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'draft')
                """,
                (
                    tenant_id, function_id, data.function_name, data.function_type,
                    data.language, data.code, data.entry_point,
                ),
            )
        return {"function_id": function_id, "function_name": data.function_name, "status": "draft"}

    def deploy_function(self, tenant_id: int, function_id: str) -> dict:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE connections_functions SET status='deployed' WHERE tenant_id=%s AND function_id=%s",
                (tenant_id, function_id),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="函数不存在")
        return {"ok": True, "function_id": function_id}

    def list_function_runs(self, tenant_id: int, function_id: str, limit: int = 50) -> list[dict]:
        limit = min(max(limit, 1), 200)
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_id, status, duration_ms, memory_mb, error_msg, created_at
                FROM connections_function_runs
                WHERE tenant_id=%s AND function_id=%s ORDER BY created_at DESC LIMIT %s
                """,
                (tenant_id, function_id, limit),
            )
            return cur.fetchall()

    # ── Pipelines ────────────────────────────────────────────────────────
    def list_pipelines(self, tenant_id: int) -> list[dict]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT pipeline_id, pipeline_name, status, last_executed_time, nodes, edges
                FROM connections_pipelines WHERE tenant_id=%s ORDER BY created_at DESC
                """,
                (tenant_id,),
            )
            rows = cur.fetchall()
        out = []
        for r in rows:
            r = self._normalize(r)
            r["node_count"] = len(r.get("nodes") or [])
            r["edge_count"] = len(r.get("edges") or [])
            r.pop("nodes", None)
            r.pop("edges", None)
            out.append(r)
        return out

    def create_pipeline(self, tenant_id: int, data: PipelineCreate) -> dict:
        pipeline_id = self._nid("pipe")
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO connections_pipelines
                    (tenant_id, pipeline_id, pipeline_name, nodes, edges, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    tenant_id, pipeline_id, data.pipeline_name,
                    json.dumps(data.nodes or [], ensure_ascii=False),
                    json.dumps(data.edges or [], ensure_ascii=False),
                    data.status,
                ),
            )
        return {"pipeline_id": pipeline_id, "pipeline_name": data.pipeline_name}

    def get_pipeline(self, tenant_id: int, pipeline_id: str) -> dict | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT pipeline_id, pipeline_name, nodes, edges, status
                FROM connections_pipelines WHERE tenant_id=%s AND pipeline_id=%s
                """,
                (tenant_id, pipeline_id),
            )
            return self._normalize(cur.fetchone())

    def execute_pipeline(self, tenant_id: int, pipeline_id: str, body: PipelineExecuteRequest) -> dict:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT pipeline_id FROM connections_pipelines WHERE tenant_id=%s AND pipeline_id=%s",
                (tenant_id, pipeline_id),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="管道不存在")
            cur.execute(
                """
                UPDATE connections_pipelines
                SET last_executed_time=NOW(), execution_count=execution_count+1
                WHERE tenant_id=%s AND pipeline_id=%s
                """,
                (tenant_id, pipeline_id),
            )
        return {"execution_id": self._nid("exec"), "status": "pending", "estimated_duration_ms": 1500}


# ════════════════════════════════════════════════════════════════════════
# Router — APIRouter（变量名 router），自行实例化 service
# ════════════════════════════════════════════════════════════════════════

router = APIRouter(prefix="/connections", tags=["connections"])
service = ConnectionsService()


def _require(row: dict | None, msg: str = "资源不存在") -> dict:
    if row is None:
        raise HTTPException(status_code=404, detail=msg)
    return row


# ── Sources ──────────────────────────────────────────────────────────────
@router.get("/sources")
def list_sources(tenant_id: int = Query(...)):
    return {"sources": service.list_sources(tenant_id)}


@router.post("/sources")
def create_source(body: SourceCreate, tenant_id: int = Query(...)):
    return service.create_source(tenant_id, body)


@router.get("/sources/{source_id}")
def get_source(source_id: str, tenant_id: int = Query(...)):
    return _require(service.get_source(tenant_id, source_id), "数据源不存在")


@router.post("/sources/{source_id}/test")
def test_source(source_id: str, body: TestConnectionRequest, tenant_id: int = Query(...)):
    return service.test_source(tenant_id, source_id, body.config)


@router.get("/sources/{source_id}/events")
def list_source_events(source_id: str, tenant_id: int = Query(...), limit: int = 50):
    return {"events": service.list_source_events(tenant_id, source_id, limit)}


@router.post("/events/track")
def track(body: TrackRequest, tenant_id: int = Query(...)):
    return service.track(tenant_id, body)


# ── Destinations ──────────────────────────────────────────────────────────
@router.get("/destinations")
def list_destinations(tenant_id: int = Query(...)):
    return {"destinations": service.list_destinations(tenant_id)}


@router.post("/destinations")
def create_destination(body: DestinationCreate, tenant_id: int = Query(...)):
    return service.create_destination(tenant_id, body)


@router.get("/destinations/{destination_id}")
def get_destination(destination_id: str, tenant_id: int = Query(...)):
    return _require(service.get_destination(tenant_id, destination_id), "目的地不存在")


@router.post("/destinations/{destination_id}/test")
def test_destination(destination_id: str, body: DestinationTestRequest, tenant_id: int = Query(...)):
    return service.test_destination(tenant_id, destination_id, body.sample_data)


@router.post("/destinations/{destination_id}/mappings")
def save_mappings(destination_id: str, body: DestinationMappingRequest, tenant_id: int = Query(...)):
    return service.save_mappings(tenant_id, destination_id, body)


# ── Reverse-ETL ────────────────────────────────────────────────────────────
@router.get("/reverse-etl/jobs")
def list_reverse_etl_jobs(tenant_id: int = Query(...)):
    return {"jobs": service.list_reverse_etl_jobs(tenant_id)}


@router.post("/reverse-etl/jobs")
def create_reverse_etl_job(body: ReverseEtlJobCreate, tenant_id: int = Query(...)):
    return service.create_reverse_etl_job(tenant_id, body)


@router.get("/reverse-etl/jobs/{job_id}/runs")
def list_reverse_etl_runs(job_id: str, tenant_id: int = Query(...), limit: int = 50):
    return {"runs": service.list_reverse_etl_runs(tenant_id, job_id, limit)}


@router.post("/reverse-etl/jobs/{job_id}/run-now")
def run_reverse_etl_now(job_id: str, tenant_id: int = Query(...)):
    return service.run_reverse_etl_now(tenant_id, job_id)


@router.post("/reverse-etl/runs/{run_id}/complete")
def complete_reverse_etl_run(run_id: str, tenant_id: int = Query(...),
                             status: str = Query("success"), error_msg: str | None = Query(None)):
    """收尾一次运行（pending → success/failed）。供调度/智能助手的后台任务回调。"""
    return service.complete_reverse_etl_run(tenant_id, run_id, status, error_msg)


# ── Warehouses ─────────────────────────────────────────────────────────────
@router.get("/warehouses")
def list_warehouses(tenant_id: int = Query(...)):
    return {"warehouses": service.list_warehouses(tenant_id)}


@router.post("/warehouses")
def create_warehouse(body: WarehouseCreate, tenant_id: int = Query(...)):
    return service.create_warehouse(tenant_id, body)


@router.post("/warehouses/{warehouse_id}/sync")
def sync_warehouse(warehouse_id: str, tenant_id: int = Query(...)):
    return service.sync_warehouse(tenant_id, warehouse_id)


# ── Functions ──────────────────────────────────────────────────────────────
@router.get("/functions")
def list_functions(tenant_id: int = Query(...)):
    return {"functions": service.list_functions(tenant_id)}


@router.post("/functions")
def create_function(body: FunctionCreate, tenant_id: int = Query(...)):
    return service.create_function(tenant_id, body)


@router.post("/functions/{function_id}/deploy")
def deploy_function(function_id: str, tenant_id: int = Query(...)):
    return service.deploy_function(tenant_id, function_id)


@router.get("/functions/{function_id}/runs")
def list_function_runs(function_id: str, tenant_id: int = Query(...), limit: int = 50):
    return {"runs": service.list_function_runs(tenant_id, function_id, limit)}


# ── Pipelines ──────────────────────────────────────────────────────────────
@router.get("/pipelines")
def list_pipelines(tenant_id: int = Query(...)):
    return {"pipelines": service.list_pipelines(tenant_id)}


@router.post("/pipelines")
def create_pipeline(body: PipelineCreate, tenant_id: int = Query(...)):
    return service.create_pipeline(tenant_id, body)


@router.get("/pipelines/{pipeline_id}")
def get_pipeline(pipeline_id: str, tenant_id: int = Query(...)):
    return _require(service.get_pipeline(tenant_id, pipeline_id), "管道不存在")


@router.post("/pipelines/{pipeline_id}/execute")
def execute_pipeline(pipeline_id: str, body: PipelineExecuteRequest, tenant_id: int = Query(...)):
    return service.execute_pipeline(tenant_id, pipeline_id, body)

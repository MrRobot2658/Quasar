"""OLAP 执行器 — 与底层存储解耦，可切换 MySQL模拟 / Doris"""

import os
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any

import pymysql


class OlapExecutor(ABC):
    @abstractmethod
    def execute(self, sql: str, params: dict[str, Any]) -> list[dict]:
        ...

    @abstractmethod
    def health(self) -> dict:
        ...


class MysqlOlapExecutor(OlapExecutor):
    """本地开发：MySQL 模拟 Doris 表（doris_user_wide / doris_id_mapping）"""

    def __init__(self):
        self.config = {
            "host": os.getenv("OLAP_HOST", "localhost"),
            "port": int(os.getenv("OLAP_PORT", "3308")),
            "user": os.getenv("OLAP_USER", "agenticdmp"),
            "password": os.getenv("OLAP_PASSWORD", "agenticdmp123"),
            "database": os.getenv("OLAP_DATABASE", "agenticdmp"),
            "charset": "utf8mb4",
            "cursorclass": pymysql.cursors.DictCursor,
        }
        self.backend = "mysql-simulated-doris"

    @contextmanager
    def _conn(self):
        conn = pymysql.connect(**self.config)
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, sql: str, params: dict[str, Any]) -> list[dict]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return list(cur.fetchall())

    def health(self) -> dict:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                cur.fetchone()
        return {"backend": self.backend, "host": self.config["host"], "status": "ok"}


class DorisOlapExecutor(OlapExecutor):
    """生产环境：通过 Doris FE MySQL 协议端口查询（默认 9030）"""

    def __init__(self):
        self.config = {
            "host": os.getenv("OLAP_HOST", "doris-fe"),
            "port": int(os.getenv("OLAP_PORT", "9030")),
            "user": os.getenv("OLAP_USER", "root"),
            "password": os.getenv("OLAP_PASSWORD", ""),
            "database": os.getenv("OLAP_DATABASE", "tenant_1001"),
            "charset": "utf8mb4",
            "cursorclass": pymysql.cursors.DictCursor,
        }
        self.backend = "doris"

    @contextmanager
    def _conn(self):
        conn = pymysql.connect(**self.config)
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, sql: str, params: dict[str, Any]) -> list[dict]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return list(cur.fetchall())

    def health(self) -> dict:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                cur.fetchone()
        return {"backend": self.backend, "host": self.config["host"], "status": "ok"}


def create_executor() -> OlapExecutor:
    backend = os.getenv("OLAP_BACKEND", "mysql").lower()
    if backend == "doris":
        return DorisOlapExecutor()
    return MysqlOlapExecutor()

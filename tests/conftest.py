"""用户画像实时 E2E 测试 — 公共配置与 fixture"""

import os
import time

import pymysql
import pytest
import redis
import requests

MYSQL_CONFIG = {
    "host": os.getenv("TEST_MYSQL_HOST", "localhost"),
    "port": int(os.getenv("TEST_MYSQL_PORT", "3308")),
    "user": os.getenv("TEST_MYSQL_USER", "agenticdmp"),
    "password": os.getenv("TEST_MYSQL_PASSWORD", "agenticdmp123"),
    "database": os.getenv("TEST_MYSQL_DATABASE", "agenticdmp"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

REDIS_HOST = os.getenv("TEST_REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("TEST_REDIS_PORT", "6381"))
API_BASE = os.getenv("TEST_API_BASE", "http://localhost:8001")
KAFKA_BOOTSTRAP = os.getenv("TEST_KAFKA_BOOTSTRAP", "localhost:9094")


def wait_for_service(url: str, timeout: float = 60.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(url, timeout=3)
            if resp.status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(2)
    return False


@pytest.fixture(scope="session")
def services_ready():
    """确保 Docker 服务已启动"""
    if not wait_for_service(f"{API_BASE}/health"):
        pytest.skip("id-mapping 服务未就绪，请先运行: docker compose up -d --build")
    yield


@pytest.fixture
def mysql_conn():
    conn = pymysql.connect(**MYSQL_CONFIG)
    yield conn
    conn.close()


@pytest.fixture
def redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


@pytest.fixture
def api_base():
    return API_BASE


@pytest.fixture
def kafka_bootstrap():
    return KAFKA_BOOTSTRAP

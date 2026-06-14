"""00-platform 模块测试：租户管理 / 每租户配置 / 配置审计

覆盖端点：
  GET    /platform/tenants                       租户列表（搜索/筛选/分页）
  POST   /platform/tenants                       新建租户
  GET    /platform/tenants/{tenant_id}           租户详情 + 配置统计
  PUT    /platform/tenants/{tenant_id}           编辑基础信息
  PATCH  /platform/tenants/{tenant_id}           停用/启用
  GET    /platform/tenants/{tenant_id}/config    获取按域合并的完整配置
  PUT    /platform/tenants/{tenant_id}/config    更新配置（dry-run 校验 + 审计）
  GET    /platform/audit/tenant-config           配置变更审计日志

前置：docker compose up -d mysql redis sql-engine
      bash scripts/apply_migrations.sh   # migrate_modules.sql（tenant_config / tenant_audit）

约定（对齐 tests/conftest.py 与 test_multi_object.py）：
  - 服务不可达（连接失败）或平台模块未部署（/platform/tenants 非 200）→ 整组 skip，不 fail。
  - 仅做加法：所有写操作都新建独立租户（tenant_name 带随机后缀），测试结束直连 MySQL
    清理这些租户的 tenants / tenant_config / tenant_audit / one_id_sequence 行，
    绝不触碰既有 1001 / 1002 / 1003。
"""

import os
import uuid

import pymysql
import pytest
import requests

# sql-engine 直连端口（与 test_multi_object.py 一致默认 8002，可用网关 :8080/api 覆盖）
API = os.getenv("TEST_SQL_ENGINE_BASE", "http://localhost:8002")

# 绕过本机 http_proxy，避免 localhost 被代理
S = requests.Session()
S.trust_env = False

MYSQL_CONFIG = {
    "host": os.getenv("TEST_MYSQL_HOST", "localhost"),
    "port": int(os.getenv("TEST_MYSQL_PORT", "3308")),
    "user": os.getenv("TEST_MYSQL_USER", "agenticdmp"),
    "password": os.getenv("TEST_MYSQL_PASSWORD", "agenticdmp123"),
    "database": os.getenv("TEST_MYSQL_DATABASE", "agenticdmp"),
    "charset": "utf8mb4",
}

CONFIG_DOMAINS = ["基础", "数据通道", "容量", "ID-Mapping", "存储", "隐私", "集成", "配额"]


def _platform_available() -> bool:
    """平台模块可达性探针：连接失败或非 200 都视为不可用 → skip。"""
    try:
        resp = S.get(f"{API}/platform/tenants", params={"limit": 1}, timeout=5)
        return resp.status_code == 200
    except requests.RequestException:
        return False


pytestmark = pytest.mark.skipif(
    not _platform_available(),
    reason="sql-engine 平台模块未就绪（请 docker compose up -d --build sql-engine + apply_migrations）",
)


# ──────────────────────────────────────────────────────────────────────────
# fixtures / helpers
# ──────────────────────────────────────────────────────────────────────────

@pytest.fixture
def created_tenants():
    """登记本测试新建的 tenant_id，结束后直连 MySQL 清理，避免污染。"""
    ids: list[int] = []
    yield ids
    if not ids:
        return
    try:
        conn = pymysql.connect(autocommit=True, **MYSQL_CONFIG)
    except pymysql.MySQLError:
        return  # 数据库不可达时静默放过，不让清理失败掩盖测试结果
    try:
        with conn.cursor() as cur:
            for tid in ids:
                for tbl in ("tenant_config", "tenant_audit", "one_id_sequence", "tenants"):
                    cur.execute(f"DELETE FROM {tbl} WHERE tenant_id = %s", (tid,))
    finally:
        conn.close()


def _new_tenant(created: list[int], **overrides) -> tuple[int, dict]:
    """创建一个带随机名的租户并登记清理，返回 (tenant_id, 请求体)。"""
    body = {
        "tenant_name": f"__pytest_{uuid.uuid4().hex[:8]}__",
        "tier": "standard",
        "scale_tier": "dev",
        "actor": "pytest",
    }
    body.update(overrides)
    resp = S.post(f"{API}/platform/tenants", json=body, timeout=10)
    assert resp.status_code == 200, resp.text
    tid = resp.json()["tenant_id"]
    created.append(tid)
    return tid, body


# ──────────────────────────────────────────────────────────────────────────
# 租户列表 / 搜索 / 筛选 / 分页
# ──────────────────────────────────────────────────────────────────────────

class TestTenantList:
    def test_list_shape(self):
        data = S.get(f"{API}/platform/tenants", timeout=10).json()
        assert "tenants" in data and "total" in data
        assert isinstance(data["tenants"], list)
        assert data["total"] >= 3  # 既有 1001/1002/1003
        sample = data["tenants"][0]
        for key in ("tenant_id", "tenant_name", "tier", "status",
                    "scale_tier", "events_count_24h"):
            assert key in sample

    def test_filter_by_status(self, created_tenants):
        _new_tenant(created_tenants)  # 至少有一个 active
        data = S.get(f"{API}/platform/tenants",
                     params={"status": "active", "limit": 200}, timeout=10).json()
        assert data["tenants"]
        assert all(t["status"] == "active" for t in data["tenants"])

    def test_filter_by_tier(self, created_tenants):
        tid, _ = _new_tenant(created_tenants, tier="premium")
        data = S.get(f"{API}/platform/tenants",
                     params={"tier": "premium", "limit": 200}, timeout=10).json()
        assert all(t["tier"] == "premium" for t in data["tenants"])
        assert tid in {t["tenant_id"] for t in data["tenants"]}

    def test_search_by_id(self, created_tenants):
        tid, _ = _new_tenant(created_tenants)
        data = S.get(f"{API}/platform/tenants",
                     params={"search": str(tid)}, timeout=10).json()
        assert tid in {t["tenant_id"] for t in data["tenants"]}

    def test_pagination_limit(self):
        data = S.get(f"{API}/platform/tenants",
                     params={"limit": 1, "offset": 0}, timeout=10).json()
        assert len(data["tenants"]) <= 1
        assert data["total"] >= 3  # total 不受 limit 影响


# ──────────────────────────────────────────────────────────────────────────
# 租户 CRUD（创建 / 详情 / 编辑 / 启停）
# ──────────────────────────────────────────────────────────────────────────

class TestTenantCrud:
    def test_create_then_detail(self, created_tenants):
        tid, body = _new_tenant(created_tenants)
        detail = S.get(f"{API}/platform/tenants/{tid}", timeout=10).json()
        assert detail["tenant_id"] == tid
        assert detail["tenant_name"] == body["tenant_name"]
        assert detail["status"] == "active"
        # 创建时自动建独占 topic
        assert detail["kafka_topic"] == f"tenant-{tid}-events"
        # 创建流程会初始化「数据通道 / 容量」两个配置域
        summary = detail["config_summary"]
        assert summary.get("数据通道", 0) >= 1
        assert summary.get("容量", 0) >= 1

    def test_update_basic_fields(self, created_tenants):
        tid, _ = _new_tenant(created_tenants)
        resp = S.put(f"{API}/platform/tenants/{tid}", json={
            "contact_email": "ops@example.com",
            "description": "由 pytest 修改",
            "tier": "premium",
            "actor": "pytest",
        }, timeout=10)
        assert resp.status_code == 200
        assert resp.json()["tenant_id"] == tid
        detail = S.get(f"{API}/platform/tenants/{tid}", timeout=10).json()
        assert detail["contact_email"] == "ops@example.com"
        assert detail["description"] == "由 pytest 修改"
        assert detail["tier"] == "premium"

    def test_patch_suspend_then_resume(self, created_tenants):
        tid, _ = _new_tenant(created_tenants)
        r1 = S.patch(f"{API}/platform/tenants/{tid}",
                     json={"status": "suspended", "reason": "测试停用", "actor": "pytest"},
                     timeout=10)
        assert r1.status_code == 200 and r1.json()["status"] == "suspended"
        assert S.get(f"{API}/platform/tenants/{tid}", timeout=10).json()["status"] == "suspended"

        r2 = S.patch(f"{API}/platform/tenants/{tid}",
                     json={"status": "active", "actor": "pytest"}, timeout=10)
        assert r2.status_code == 200 and r2.json()["status"] == "active"
        assert S.get(f"{API}/platform/tenants/{tid}", timeout=10).json()["status"] == "active"


# ──────────────────────────────────────────────────────────────────────────
# 每租户配置（读取按域合并 / 更新 + dry-run / 同步主表）
# ──────────────────────────────────────────────────────────────────────────

class TestTenantConfig:
    def test_get_config_all_domains(self, created_tenants):
        tid, body = _new_tenant(created_tenants)
        cfg = S.get(f"{API}/platform/tenants/{tid}/config", timeout=10).json()
        assert cfg["tenant_id"] == tid
        for domain in CONFIG_DOMAINS:
            assert domain in cfg
        # 基础域恒从 tenants 主表派生
        assert cfg["基础"]["tenant_name"] == body["tenant_name"]
        assert cfg["基础"]["kafka_topic"] == f"tenant-{tid}-events"

    def test_get_config_single_domain(self, created_tenants):
        tid, _ = _new_tenant(created_tenants)
        cfg = S.get(f"{API}/platform/tenants/{tid}/config",
                    params={"domain": "ID-Mapping"}, timeout=10).json()
        assert cfg["tenant_id"] == tid
        assert "ID-Mapping" in cfg
        # 单域读取不应返回其它业务域（基础域可派生，不强校验）
        assert "存储" not in cfg

    def test_update_config_roundtrip(self, created_tenants):
        tid, _ = _new_tenant(created_tenants)
        resp = S.put(f"{API}/platform/tenants/{tid}/config", json={
            "domain": "ID-Mapping",
            "updates": {"confidence_threshold": 0.75, "algo_merge": True},
            "reason": "调阈值",
            "actor": "pytest",
        }, timeout=10)
        assert resp.status_code == 200
        out = resp.json()
        assert out["domain"] == "ID-Mapping"
        assert set(out["updated_keys"]) == {"confidence_threshold", "algo_merge"}
        # 回读应反映写入值
        cfg = S.get(f"{API}/platform/tenants/{tid}/config",
                    params={"domain": "ID-Mapping"}, timeout=10).json()
        assert cfg["ID-Mapping"]["confidence_threshold"] == 0.75
        assert cfg["ID-Mapping"]["algo_merge"] is True

    def test_update_capacity_syncs_main_table(self, created_tenants):
        """容量域 scale_tier 应同步回 tenants 主表（详情可见）。"""
        tid, _ = _new_tenant(created_tenants)
        resp = S.put(f"{API}/platform/tenants/{tid}/config", json={
            "domain": "容量", "updates": {"scale_tier": "medium"}, "actor": "pytest",
        }, timeout=10)
        assert resp.status_code == 200
        assert S.get(f"{API}/platform/tenants/{tid}", timeout=10).json()["scale_tier"] == "medium"


# ──────────────────────────────────────────────────────────────────────────
# 边界 / 校验
# ──────────────────────────────────────────────────────────────────────────

class TestBoundaries:
    MISSING = 999999  # 约定不存在的租户

    def test_detail_not_found(self):
        assert S.get(f"{API}/platform/tenants/{self.MISSING}", timeout=10).status_code == 404

    def test_update_not_found(self):
        r = S.put(f"{API}/platform/tenants/{self.MISSING}",
                  json={"tenant_name": "x", "actor": "pytest"}, timeout=10)
        assert r.status_code == 404

    def test_patch_not_found(self):
        r = S.patch(f"{API}/platform/tenants/{self.MISSING}",
                    json={"status": "suspended", "actor": "pytest"}, timeout=10)
        assert r.status_code == 404

    def test_get_config_not_found(self):
        r = S.get(f"{API}/platform/tenants/{self.MISSING}/config", timeout=10)
        assert r.status_code == 404

    def test_update_config_not_found(self):
        r = S.put(f"{API}/platform/tenants/{self.MISSING}/config",
                  json={"domain": "隐私", "updates": {"retention_days": 30}}, timeout=10)
        assert r.status_code == 404

    def test_invalid_status_rejected(self, created_tenants):
        tid, _ = _new_tenant(created_tenants)
        r = S.patch(f"{API}/platform/tenants/{tid}",
                    json={"status": "weird", "actor": "pytest"}, timeout=10)
        assert r.status_code == 400

    def test_invalid_domain_rejected(self, created_tenants):
        tid, _ = _new_tenant(created_tenants)
        r = S.put(f"{API}/platform/tenants/{tid}/config",
                  json={"domain": "BOGUS", "updates": {"a": 1}}, timeout=10)
        assert r.status_code == 400

    def test_empty_updates_rejected(self, created_tenants):
        tid, _ = _new_tenant(created_tenants)
        r = S.put(f"{API}/platform/tenants/{tid}/config",
                  json={"domain": "配额", "updates": {}}, timeout=10)
        assert r.status_code == 400

    def test_kafka_topic_conflict_rejected(self, created_tenants):
        """dry-run 校验：kafka_topic 不得与其它租户冲突。"""
        tid, _ = _new_tenant(created_tenants)
        # 1001 占用 tenant-1001-events
        r = S.put(f"{API}/platform/tenants/{tid}/config", json={
            "domain": "数据通道",
            "updates": {"kafka_topic": "tenant-1001-events"},
            "actor": "pytest",
        }, timeout=10)
        assert r.status_code == 400


# ──────────────────────────────────────────────────────────────────────────
# 多租户隔离 + 审计
# ──────────────────────────────────────────────────────────────────────────

class TestMultiTenantIsolation:
    def test_config_isolation(self, created_tenants):
        """A 改 ID-Mapping，不应泄漏到 B。"""
        a, _ = _new_tenant(created_tenants)
        b, _ = _new_tenant(created_tenants)
        S.put(f"{API}/platform/tenants/{a}/config", json={
            "domain": "ID-Mapping",
            "updates": {"confidence_threshold": 0.9},
            "actor": "pytest",
        }, timeout=10).raise_for_status()

        cfg_a = S.get(f"{API}/platform/tenants/{a}/config",
                      params={"domain": "ID-Mapping"}, timeout=10).json()
        cfg_b = S.get(f"{API}/platform/tenants/{b}/config",
                      params={"domain": "ID-Mapping"}, timeout=10).json()
        assert cfg_a["ID-Mapping"].get("confidence_threshold") == 0.9
        assert "confidence_threshold" not in cfg_b["ID-Mapping"]

    def test_audit_scoped_by_tenant(self, created_tenants):
        """审计按 tenant_id 隔离：A 的查询只返回 A 的记录。"""
        a, _ = _new_tenant(created_tenants)
        b, _ = _new_tenant(created_tenants)
        # 给 B 也制造一条审计（应不出现在 A 的查询里）
        S.put(f"{API}/platform/tenants/{b}/config", json={
            "domain": "隐私", "updates": {"retention_days": 90}, "actor": "pytest",
        }, timeout=10).raise_for_status()

        data = S.get(f"{API}/platform/audit/tenant-config",
                     params={"tenant_id": a, "limit": 200}, timeout=10).json()
        assert "audits" in data and "total" in data
        assert data["total"] >= 1  # 至少有创建时写入的 create 审计
        assert all(row["tenant_id"] == a for row in data["audits"])
        # 创建动作必然在 A 的审计里
        assert any(row["action"] == "create" for row in data["audits"])

    def test_audit_filter_by_action(self, created_tenants):
        a, _ = _new_tenant(created_tenants)
        data = S.get(f"{API}/platform/audit/tenant-config",
                     params={"tenant_id": a, "action": "create"}, timeout=10).json()
        assert all(row["action"] == "create" for row in data["audits"])

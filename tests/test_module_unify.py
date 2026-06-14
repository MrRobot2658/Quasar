"""02 · unify 模块端点测试（对标 Twilio Segment 的 Unify / Identity / Computed Traits）

覆盖 unify_api.py 的全部新增端点：
  身份解析规则 / 任意对象打标 / 泛对象群组刷新 / SQL 特征 /
  预测模型 / 档案回流 / 泛对象搜索（叠加标签过滤）。

设计原则（与既有测试一致）：
  - 只读/只写自有“独立数据”，使用专用测试租户避免污染既有 1001/1002。
  - 服务不可达或 unify 路由尚未部署（容器未重建）→ skip，不 fail。
  - 走 sql-engine（宿主 :8002），绕过本机 http_proxy（localhost 不走代理）。
  - 每条用例正向 + 边界 + 多租户隔离。
  - 不修改任何既有测试。
"""

import json

import pymysql
import pytest
import requests

API = "http://localhost:8002"

# 专用测试租户（与既有 1001/1002 隔离）；OTHER 用于多租户隔离断言
TENANT = 990201
OTHER = 990202

# 绕过本机 http_proxy，避免 localhost 被代理
S = requests.Session()
S.trust_env = False

# 直连 MySQL（与 conftest 一致：宿主 3308），仅用于群组 fixture 搭建与收尾清理
MYSQL_CONFIG = {
    "host": "localhost", "port": 3308, "user": "agenticdmp",
    "password": "agenticdmp123", "database": "agenticdmp",
    "charset": "utf8mb4", "cursorclass": pymysql.cursors.DictCursor,
}

# 测试中创建的固定资源标识（前缀 ut_ = unify test，便于清理/复跑幂等）
LEAD_A = "UT_LEAD_A"
LEAD_B = "UT_LEAD_B"
TAG_VIP = "ut_vip"
TAG_NEW = "ut_new"
GROUP_ID = 990201001          # 固定大值，避开 user_groups 自增区间，便于清理
TRAIT_CODE = "ut_city_trait"
MODEL_NAME = "ut_churn_model"


# ── 可用性探测 / skip 闸 ──────────────────────────────────────────────────

def _service_up() -> bool:
    try:
        return S.get(f"{API}/health", timeout=3).status_code == 200
    except requests.RequestException:
        return False


def _unify_deployed() -> bool:
    """unify 路由是否已部署：GET 一个 unify 路由，404 视为未部署（容器需重建）。"""
    try:
        r = S.get(f"{API}/unify/identity-rules/{TENANT}", timeout=3)
        return r.status_code != 404
    except requests.RequestException:
        return False


@pytest.fixture(scope="module", autouse=True)
def unify_ready():
    if not _service_up():
        pytest.skip("sql-engine 未就绪（:8002），请先 docker compose up -d --build")
    if not _unify_deployed():
        pytest.skip("unify 路由未部署，请重建 sql-engine 容器：docker compose up -d --build sql-engine")
    _seed()
    yield
    _cleanup()


def _mysql():
    return pymysql.connect(**MYSQL_CONFIG, autocommit=True)


def _seed():
    """搭建独立数据：两条线索 + 一个动态群组（直连 MySQL 仅做 fixture 准备）。"""
    # 线索经 objects/upsert 接入（走既有 validate 路径，不手拼业务 SQL）
    for lid, city in ((LEAD_A, "测试城甲"), (LEAD_B, "测试城甲")):
        S.post(f"{API}/objects/upsert", json={
            "tenant_id": TENANT, "object": "lead",
            "record": {"lead_id": lid, "lead_name": "unify测试线索", "city": city,
                       "company_size": 600, "source": "ut", "stage": "new"},
        }, timeout=10).raise_for_status()
    # 动态群组直接落 user_groups（unify 模块只提供 refresh，不提供建群）
    try:
        with _mysql() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO user_groups "
                "(tenant_id, group_id, group_code, group_name, group_type, filter_rule, member_object_type) "
                "VALUES (%s, %s, %s, %s, 'dynamic', %s, 'lead') "
                "ON DUPLICATE KEY UPDATE filter_rule=VALUES(filter_rule), group_type='dynamic'",
                (TENANT, GROUP_ID, "ut_dyn_group", "unify动态群组",
                 json.dumps({"object": "lead",
                             "conditions": [{"field": "city", "op": "eq", "value": "测试城甲"}]},
                            ensure_ascii=False)),
            )
    except pymysql.MySQLError:
        pass  # 群组类用例自带 mysql skip 守卫


def _cleanup():
    """收尾：删除本测试租户在各 unify 表与对象表的数据，避免污染。"""
    try:
        with _mysql() as conn, conn.cursor() as cur:
            for t in (TENANT, OTHER):
                for tbl in ("identity_resolution_rules", "object_tags", "object_group_members",
                            "sql_trait_definitions", "sql_trait_results", "prediction_models",
                            "connections_reverse_etl_jobs", "connections_reverse_etl_runs",
                            "user_groups", "object_lead"):
                    cur.execute(f"DELETE FROM {tbl} WHERE tenant_id=%s", (t,))
    except pymysql.MySQLError:
        pass


@pytest.fixture
def mysql_required():
    """需要直连 MySQL 的用例：连不上则 skip。"""
    try:
        conn = _mysql()
    except pymysql.MySQLError:
        pytest.skip("MySQL 未就绪（:3308）")
    yield conn
    conn.close()


# ── 身份解析规则 ──────────────────────────────────────────────────────────

class TestIdentityRules:
    def test_upsert_and_list(self):
        r = S.post(f"{API}/unify/identity-rules/{TENANT}",
                   json={"identifier_type": "phone", "priority": 10, "is_primary": True,
                         "merge_strategy": "latest", "description": "手机号主标识"}, timeout=10)
        assert r.status_code == 201
        assert r.json()["rule_id"] == "rule_phone"
        rules = S.get(f"{API}/unify/identity-rules/{TENANT}", timeout=10).json()
        assert any(x["rule_id"] == "rule_phone" and x["priority"] == 10 for x in rules)

    def test_upsert_is_idempotent_update(self):
        """同 identifier_type 二次提交应更新而非重复（ON DUPLICATE KEY）。"""
        S.post(f"{API}/unify/identity-rules/{TENANT}",
               json={"identifier_type": "email", "priority": 30}, timeout=10).raise_for_status()
        S.post(f"{API}/unify/identity-rules/{TENANT}",
               json={"identifier_type": "email", "priority": 99}, timeout=10).raise_for_status()
        rules = [x for x in S.get(f"{API}/unify/identity-rules/{TENANT}", timeout=10).json()
                 if x["identifier_type"] == "email"]
        assert len(rules) == 1 and rules[0]["priority"] == 99

    def test_explicit_rule_id_respected(self):
        r = S.post(f"{API}/unify/identity-rules/{TENANT}",
                   json={"rule_id": "ut_dev_rule", "identifier_type": "device"}, timeout=10)
        assert r.status_code == 201 and r.json()["rule_id"] == "ut_dev_rule"

    def test_delete_and_missing(self):
        S.post(f"{API}/unify/identity-rules/{TENANT}",
               json={"rule_id": "ut_tmp_rule", "identifier_type": "wechat_openid"}, timeout=10).raise_for_status()
        ok = S.delete(f"{API}/unify/identity-rules/{TENANT}/ut_tmp_rule", timeout=10)
        assert ok.status_code == 200 and ok.json()["ok"] is True
        # 再删 → 404
        assert S.delete(f"{API}/unify/identity-rules/{TENANT}/ut_tmp_rule", timeout=10).status_code == 404

    def test_tenant_isolation(self):
        S.post(f"{API}/unify/identity-rules/{TENANT}",
               json={"rule_id": "ut_iso_rule", "identifier_type": "unionid"}, timeout=10).raise_for_status()
        other = S.get(f"{API}/unify/identity-rules/{OTHER}", timeout=10).json()
        assert all(x["rule_id"] != "ut_iso_rule" for x in other)


# ── 任意对象打标 ──────────────────────────────────────────────────────────

class TestObjectTags:
    def test_assign_and_get(self):
        r = S.post(f"{API}/unify/tags/{TENANT}/{TAG_VIP}/assign",
                   json={"object_type": "lead", "object_id": LEAD_A, "source": "manual"}, timeout=10)
        assert r.status_code == 201
        got = S.get(f"{API}/unify/object-tags/{TENANT}/lead/{LEAD_A}", timeout=10).json()
        assert any(t["tag_code"] == TAG_VIP for t in got["tags"])

    def test_assign_unknown_object_rejected(self):
        r = S.post(f"{API}/unify/tags/{TENANT}/{TAG_VIP}/assign",
                   json={"object_type": "bogus", "object_id": "x"}, timeout=10)
        assert r.status_code == 400

    def test_remove_and_missing(self):
        S.post(f"{API}/unify/tags/{TENANT}/{TAG_NEW}/assign",
               json={"object_type": "lead", "object_id": LEAD_B}, timeout=10).raise_for_status()
        ok = S.delete(f"{API}/unify/tags/{TENANT}/{TAG_NEW}/object/lead/{LEAD_B}", timeout=10)
        assert ok.status_code == 200 and ok.json()["ok"] is True
        # 再删 → 404
        assert S.delete(f"{API}/unify/tags/{TENANT}/{TAG_NEW}/object/lead/{LEAD_B}",
                        timeout=10).status_code == 404

    def test_tenant_isolation(self):
        # TENANT 打标，OTHER 同对象不应看到
        S.post(f"{API}/unify/tags/{TENANT}/{TAG_VIP}/assign",
               json={"object_type": "lead", "object_id": LEAD_A}, timeout=10).raise_for_status()
        other = S.get(f"{API}/unify/object-tags/{OTHER}/lead/{LEAD_A}", timeout=10).json()
        assert other["tags"] == []


# ── 泛对象群组刷新 ────────────────────────────────────────────────────────

class TestGroups:
    def test_list_groups(self, mysql_required):
        groups = S.get(f"{API}/unify/groups/{TENANT}", timeout=10).json()
        assert isinstance(groups, list)
        assert any(g["group_id"] == GROUP_ID for g in groups)

    def test_list_groups_filter_type(self, mysql_required):
        dyn = S.get(f"{API}/unify/groups/{TENANT}", params={"filter_type": "dynamic"}, timeout=10).json()
        assert all(g["group_type"] == "dynamic" for g in dyn)
        assert any(g["group_id"] == GROUP_ID for g in dyn)

    def test_refresh_dynamic_group(self, mysql_required):
        """动态群组刷新应通过 ObjectService 圈出两条测试线索并落成员表。"""
        r = S.post(f"{API}/unify/groups/{TENANT}/{GROUP_ID}/refresh", timeout=20)
        assert r.status_code == 200
        body = r.json()
        assert body["object_type"] == "lead"
        assert body["matched"] >= 2 and body["member_count"] >= 2
        # 校验成员表确实落库（多租户隔离：仅 TENANT）
        with mysql_required.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS c FROM object_group_members "
                "WHERE tenant_id=%s AND group_id=%s AND object_type='lead'",
                (TENANT, GROUP_ID))
            assert cur.fetchone()["c"] >= 2

    def test_refresh_nonexistent_group(self, mysql_required):
        assert S.post(f"{API}/unify/groups/{TENANT}/99999999/refresh", timeout=10).status_code == 400


# ── SQL 特征 ──────────────────────────────────────────────────────────────

class TestSqlTraits:
    def test_create_and_list(self):
        r = S.post(f"{API}/unify/sql-traits/{TENANT}", json={
            "trait_code": TRAIT_CODE, "trait_name": "线索城市",
            "sql_query": "SELECT lead_id AS object_id, city AS trait_value, 'lead' AS object_type "
                         "FROM object_lead WHERE tenant_id=%(tenant_id)s",
            "object_type": "lead",
        }, timeout=10)
        assert r.status_code == 201
        trait_id = r.json()["trait_id"]
        traits = S.get(f"{API}/unify/sql-traits/{TENANT}", timeout=10).json()
        assert any(t["trait_id"] == trait_id and t["trait_code"] == TRAIT_CODE for t in traits)

    def test_execute_single_trait(self):
        r = S.post(f"{API}/unify/sql-traits/{TENANT}", json={
            "trait_code": "ut_exec_trait",
            "sql_query": "SELECT lead_id AS object_id, stage AS trait_value, 'lead' AS object_type "
                         "FROM object_lead WHERE tenant_id=%(tenant_id)s",
        }, timeout=10)
        trait_id = r.json()["trait_id"]
        ex = S.post(f"{API}/unify/sql-traits/{TENANT}/{trait_id}/execute", timeout=20)
        assert ex.status_code == 200
        body = ex.json()
        assert body["executed"] == 1 and body["row_count"] >= 2

    def test_execute_all_traits(self):
        ex = S.post(f"{API}/unify/sql-traits/{TENANT}/execute", json={}, timeout=20)
        assert ex.status_code == 200
        assert ex.json()["executed"] >= 1

    def test_readonly_guard_rejects_non_select(self):
        """非 SELECT 的 SQL 特征在执行时被安全闸拦截 → 400。"""
        r = S.post(f"{API}/unify/sql-traits/{TENANT}", json={
            "trait_code": "ut_bad_trait",
            "sql_query": "DELETE FROM object_lead WHERE tenant_id=1",
        }, timeout=10)
        trait_id = r.json()["trait_id"]
        assert S.post(f"{API}/unify/sql-traits/{TENANT}/{trait_id}/execute", timeout=10).status_code == 400

    def test_execute_missing_trait(self):
        assert S.post(f"{API}/unify/sql-traits/{TENANT}/ut_no_such/execute", timeout=10).status_code == 400

    def test_tenant_isolation(self):
        other = S.get(f"{API}/unify/sql-traits/{OTHER}", timeout=10).json()
        assert all(t["trait_code"] != TRAIT_CODE for t in other)


# ── 预测模型 ──────────────────────────────────────────────────────────────

class TestPredictions:
    def test_create_and_list(self):
        r = S.post(f"{API}/unify/predictions/{TENANT}", json={
            "model_name": MODEL_NAME, "model_type": "churn",
            "features": ["recency", "frequency"], "inference_horizon": "30d",
        }, timeout=10)
        assert r.status_code == 201
        body = r.json()
        assert body["model_type"] == "churn"
        assert body["features"] == ["recency", "frequency"]  # JSON 已反序列化
        models = S.get(f"{API}/unify/predictions/{TENANT}", timeout=10).json()
        assert any(m["model_name"] == MODEL_NAME for m in models)

    def test_infer(self):
        r = S.post(f"{API}/unify/predictions/{TENANT}", json={
            "model_name": "ut_purchase_model", "model_type": "purchase",
        }, timeout=10)
        model_id = r.json()["model_id"]
        inf = S.post(f"{API}/unify/predictions/{TENANT}/{model_id}/infer", timeout=30)
        assert inf.status_code == 200
        body = inf.json()
        assert body["property_key"].startswith("$.pred_")
        assert body["row_count"] >= 0  # 该测试租户宽表可能无数据，端点应仍成功
        assert "quality_score" in body

    def test_infer_missing_model(self):
        assert S.post(f"{API}/unify/predictions/{TENANT}/ut_no_model/infer", timeout=10).status_code == 400

    def test_tenant_isolation(self):
        other = S.get(f"{API}/unify/predictions/{OTHER}", timeout=10).json()
        assert all(m["model_name"] != MODEL_NAME for m in other)


# ── 档案回流（Reverse-ETL）────────────────────────────────────────────────

class TestProfilesSync:
    def test_sync(self):
        r = S.post(f"{API}/unify/profiles/sync/{TENANT}", json={
            "job_name": "ut-profile-sync", "target_warehouse": "ut_wh",
            "source_object": "doris_user_wide", "tables": ["traits", "tags"],
        }, timeout=20)
        assert r.status_code == 201
        body = r.json()
        assert body["status"] == "success"
        assert body["target_warehouse"] == "ut_wh"
        assert body["run_id"] and body["job_id"]
        assert body["row_count"] >= 0


# ── 泛对象搜索（叠加 object_tags 标签过滤）────────────────────────────────

class TestObjectSearchWithTags:
    def test_search_without_tags(self):
        """无标签 → 退化为既有 ObjectService 搜索。"""
        r = S.post(f"{API}/unify/objects/search", json={
            "tenant_id": TENANT, "object": "lead",
            "conditions": [{"field": "city", "op": "eq", "value": "测试城甲"}],
        }, timeout=15)
        assert r.status_code == 200
        ids = {row["lead_id"] for row in r.json()["data"]}
        assert {LEAD_A, LEAD_B}.issubset(ids)

    def test_search_by_tag(self):
        """打标后按 tag_codes 过滤：仅 LEAD_A 命中 ut_vip。"""
        S.post(f"{API}/unify/tags/{TENANT}/{TAG_VIP}/assign",
               json={"object_type": "lead", "object_id": LEAD_A}, timeout=10).raise_for_status()
        r = S.post(f"{API}/unify/objects/search", json={
            "tenant_id": TENANT, "object": "lead", "tag_codes": [TAG_VIP],
        }, timeout=15)
        assert r.status_code == 200
        body = r.json()
        ids = {row["lead_id"] for row in body["data"]}
        assert LEAD_A in ids and LEAD_B not in ids
        # 命中行回填了 object_tags
        row_a = next(row for row in body["data"] if row["lead_id"] == LEAD_A)
        assert any(t["tag_code"] == TAG_VIP for t in row_a["object_tags"])

    def test_search_by_tag_count_only(self):
        S.post(f"{API}/unify/tags/{TENANT}/{TAG_VIP}/assign",
               json={"object_type": "lead", "object_id": LEAD_A}, timeout=10).raise_for_status()
        r = S.post(f"{API}/unify/objects/search", json={
            "tenant_id": TENANT, "object": "lead", "tag_codes": [TAG_VIP], "count_only": True,
        }, timeout=15)
        assert r.status_code == 200
        assert r.json()["estimate"] >= 1

    def test_search_tag_logic_and(self):
        """AND 逻辑：需同时拥有两个标签才命中。"""
        S.post(f"{API}/unify/tags/{TENANT}/{TAG_VIP}/assign",
               json={"object_type": "lead", "object_id": LEAD_A}, timeout=10).raise_for_status()
        S.post(f"{API}/unify/tags/{TENANT}/{TAG_NEW}/assign",
               json={"object_type": "lead", "object_id": LEAD_A}, timeout=10).raise_for_status()
        # 仅给 A 打两标签；B 不打 → AND 只命中 A
        r = S.post(f"{API}/unify/objects/search", json={
            "tenant_id": TENANT, "object": "lead",
            "tag_codes": [TAG_VIP, TAG_NEW], "tag_logic": "and",
        }, timeout=15)
        assert r.status_code == 200
        ids = {row["lead_id"] for row in r.json()["data"]}
        assert LEAD_A in ids and LEAD_B not in ids

    def test_search_unknown_object_rejected(self):
        r = S.post(f"{API}/unify/objects/search", json={
            "tenant_id": TENANT, "object": "bogus",
        }, timeout=10)
        assert r.status_code == 400

    def test_tenant_isolation(self):
        """OTHER 租户即便用相同标签也搜不到 TENANT 的线索。"""
        r = S.post(f"{API}/unify/objects/search", json={
            "tenant_id": OTHER, "object": "lead", "tag_codes": [TAG_VIP],
        }, timeout=15)
        assert r.status_code == 200
        body = r.json()
        # OTHER 无任何打标对象
        assert body.get("data", []) == [] and body.get("matched", 0) == 0

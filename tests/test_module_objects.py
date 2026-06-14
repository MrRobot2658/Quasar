"""03-objects 模块测试 — 对象/字段/关系元数据 CRUD + 单对象详情/一跳关联。

覆盖 objects_api.py 路由（对标 Twilio Segment 的对象/Schema 管理）：
    GET    /objects/{tenant_id}/definitions
    POST   /objects/create
    POST   /objects/{tenant_id}/relations
    DELETE /objects/{tenant_id}/relations/{src_type}/{rel_type}/{dst_type}
    POST   /objects/{tenant_id}/{object_key}/fields
    PATCH  /objects/{tenant_id}/{object_key}/fields/{field_code}
    GET    /objects/{tenant_id}/{object_key}/{pk_value}/relations
    GET    /objects/{tenant_id}/{object_key}/{pk_value}

约定：
- 服务不可达（sql-engine :8002）→ 整模块 skip，不判失败。
- 全部使用独立的高位测试租户 + 每轮随机 object_key，避免污染既有数据/相互冲突。
- 测试结束后尽力（best-effort）清理自建物理表与注册行，保持库干净。

前置：docker compose up -d mysql redis sql-engine
      bash scripts/apply_migrations.sh   # 应用 migrate_modules.sql（object_definitions 等）
"""

import os
import uuid

import pytest
import requests

API = os.getenv("TEST_SQL_ENGINE_BASE", "http://localhost:8002")

# 绕过本机 http_proxy，避免 localhost 被代理（与 test_multi_object.py 一致）
S = requests.Session()
S.trust_env = False

# 独立测试租户：高位段，避免与 1001/1002 等业务数据冲突
TENANT_A = 990350
TENANT_B = 990351  # 用于多租户隔离对照

# 每轮唯一对象 key，避免重复创建报“已存在”，也便于精准清理
RUN = uuid.uuid4().hex[:8]

# 记录本轮创建的自建对象，供 teardown 清理：(tenant_id, object_key, table_name)
_CREATED: list[tuple[int, str, str]] = []


# ── 服务可达性 + 清理 ────────────────────────────────────────────────────────
@pytest.fixture(scope="module", autouse=True)
def _module_guard():
    try:
        resp = S.get(f"{API}/health", timeout=3)
        if resp.status_code != 200:
            pytest.skip(f"sql-engine 未就绪（/health={resp.status_code}）")
    except requests.RequestException:
        pytest.skip("sql-engine 服务不可达，请先运行: docker compose up -d --build")
    yield
    _cleanup()


def _cleanup():
    """尽力清理本轮自建对象的物理表与注册行；连不上 MySQL 则静默跳过。"""
    try:
        import pymysql
    except ImportError:
        return
    cfg = {
        "host": os.getenv("TEST_MYSQL_HOST", "localhost"),
        "port": int(os.getenv("TEST_MYSQL_PORT", "3308")),
        "user": os.getenv("TEST_MYSQL_USER", "agenticdmp"),
        "password": os.getenv("TEST_MYSQL_PASSWORD", "agenticdmp123"),
        "database": os.getenv("TEST_MYSQL_DATABASE", "agenticdmp"),
        "charset": "utf8mb4",
    }
    try:
        conn = pymysql.connect(**cfg, autocommit=True)
    except Exception:
        return
    try:
        with conn.cursor() as cur:
            for tenant, key, table in _CREATED:
                for stmt, args in (
                    (f"DROP TABLE IF EXISTS `{table}`", ()),
                    ("DELETE FROM object_fields WHERE tenant_id=%s AND object_key=%s", (tenant, key)),
                    ("DELETE FROM object_definitions WHERE tenant_id=%s AND object_key=%s", (tenant, key)),
                    ("DELETE FROM relation_definitions WHERE tenant_id=%s AND (src_type=%s OR dst_type=%s)", (tenant, key, key)),
                    ("DELETE FROM relation_properties WHERE tenant_id=%s AND (src_type=%s OR dst_type=%s)", (tenant, key, key)),
                ):
                    try:
                        cur.execute(stmt, args)
                    except Exception:
                        pass
            # 清理隔离/详情测试中 upsert 的 lead 与关系
            for tenant in (TENANT_A, TENANT_B):
                for stmt in (
                    "DELETE FROM object_lead WHERE tenant_id=%s AND lead_id LIKE %s",
                    "DELETE FROM doris_user_wide WHERE tenant_id=%s AND one_id BETWEEN 990350000 AND 990359999",
                    "DELETE FROM object_relations WHERE tenant_id=%s AND src_id LIKE %s",
                ):
                    try:
                        if "LIKE" in stmt and "lead" in stmt:
                            cur.execute(stmt, (tenant, f"QA_{RUN}%"))
                        elif "LIKE" in stmt:
                            cur.execute(stmt, (tenant, f"QA_{RUN}%"))
                        else:
                            cur.execute(stmt, (tenant,))
                    except Exception:
                        pass
    finally:
        conn.close()


# ── helpers ──────────────────────────────────────────────────────────────────
def _new_key() -> str:
    return f"qa_{RUN}_{uuid.uuid4().hex[:6]}"


def _create_object(tenant: int, key: str, fields=None, **extra) -> requests.Response:
    body = {"tenant_id": tenant, "object_key": key, "initial_fields": fields or [], **extra}
    resp = S.post(f"{API}/objects/create", json=body, timeout=15)
    if resp.status_code == 200:
        _CREATED.append((tenant, key, resp.json()["table_name"]))
    return resp


@pytest.fixture
def custom_object():
    """创建一个自建对象（带 name 字段），返回 object_key。teardown 由模块级 _cleanup 统一处理。"""
    key = _new_key()
    resp = _create_object(TENANT_A, key, fields=[{"code": "name", "type": "str"}])
    assert resp.status_code == 200, resp.text
    return key


# ── definitions ──────────────────────────────────────────────────────────────
class TestDefinitions:
    def test_definitions_returns_list(self):
        resp = S.get(f"{API}/objects/{TENANT_A}/definitions", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert data["tenant_id"] == TENANT_A
        assert isinstance(data["definitions"], list)

    def test_fresh_tenant_has_no_custom_objects(self):
        """从未使用过的随机租户，自建对象清单应为空。"""
        fresh = 990300000 + int(RUN[:4], 16) % 100000
        resp = S.get(f"{API}/objects/{fresh}/definitions", timeout=10)
        assert resp.status_code == 200
        assert resp.json()["definitions"] == []

    def test_created_object_appears_in_definitions(self, custom_object):
        resp = S.get(f"{API}/objects/{TENANT_A}/definitions", timeout=10)
        keys = {d["object_key"] for d in resp.json()["definitions"]}
        assert custom_object in keys
        defn = next(d for d in resp.json()["definitions"] if d["object_key"] == custom_object)
        assert defn["is_builtin"] is False
        assert {f["code"] for f in defn["fields"]} == {"name"}


# ── create object ────────────────────────────────────────────────────────────
class TestCreateObject:
    def test_create_ok(self):
        key = _new_key()
        resp = _create_object(TENANT_A, key, fields=[{"code": "score", "type": "int"}])
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["ok"] is True
        assert body["object_key"] == key
        assert body["table_name"] == f"object_{key}"

    def test_duplicate_rejected(self, custom_object):
        resp = _create_object(TENANT_A, custom_object)
        assert resp.status_code == 400
        assert "已存在" in resp.json()["detail"]

    def test_builtin_object_rejected(self):
        resp = S.post(f"{API}/objects/create",
                      json={"tenant_id": TENANT_A, "object_key": "user"}, timeout=10)
        assert resp.status_code == 400
        assert "内置" in resp.json()["detail"]

    def test_illegal_identifier_rejected(self):
        resp = S.post(f"{API}/objects/create",
                      json={"tenant_id": TENANT_A, "object_key": "Bad Name"}, timeout=10)
        assert resp.status_code == 400

    def test_reserved_table_rejected(self):
        resp = S.post(f"{API}/objects/create", json={
            "tenant_id": TENANT_A, "object_key": _new_key(),
            "table_name": "object_relations",
        }, timeout=10)
        assert resp.status_code == 400
        assert "保留" in resp.json()["detail"]

    def test_bad_field_type_rejected(self):
        resp = S.post(f"{API}/objects/create", json={
            "tenant_id": TENANT_A, "object_key": _new_key(),
            "initial_fields": [{"code": "x", "type": "blob"}],
        }, timeout=10)
        assert resp.status_code == 400


# ── fields ───────────────────────────────────────────────────────────────────
class TestFields:
    def test_add_field_ok(self, custom_object):
        resp = S.post(f"{API}/objects/{TENANT_A}/{custom_object}/fields",
                      json={"field_code": "amount", "field_type": "float"}, timeout=10)
        assert resp.status_code == 200, resp.text
        assert resp.json()["field_code"] == "amount"
        # 字段落入 definitions
        defs = S.get(f"{API}/objects/{TENANT_A}/definitions", timeout=10).json()["definitions"]
        defn = next(d for d in defs if d["object_key"] == custom_object)
        assert "amount" in {f["code"] for f in defn["fields"]}

    def test_add_field_to_builtin_rejected(self):
        resp = S.post(f"{API}/objects/{TENANT_A}/lead/fields",
                      json={"field_code": "extra", "field_type": "str"}, timeout=10)
        assert resp.status_code == 400

    def test_add_duplicate_field_rejected(self, custom_object):
        S.post(f"{API}/objects/{TENANT_A}/{custom_object}/fields",
               json={"field_code": "dupf", "field_type": "str"}, timeout=10).raise_for_status()
        resp = S.post(f"{API}/objects/{TENANT_A}/{custom_object}/fields",
                      json={"field_code": "dupf", "field_type": "str"}, timeout=10)
        assert resp.status_code == 400
        assert "已存在" in resp.json()["detail"]

    def test_add_field_bad_type_rejected(self, custom_object):
        resp = S.post(f"{API}/objects/{TENANT_A}/{custom_object}/fields",
                      json={"field_code": "weird", "field_type": "blob"}, timeout=10)
        assert resp.status_code == 400

    def test_patch_field_label_ok(self, custom_object):
        S.post(f"{API}/objects/{TENANT_A}/{custom_object}/fields",
               json={"field_code": "label_me", "field_type": "str"}, timeout=10).raise_for_status()
        resp = S.patch(f"{API}/objects/{TENANT_A}/{custom_object}/fields/label_me",
                       json={"field_label": "中文标签"}, timeout=10)
        assert resp.status_code == 200
        defs = S.get(f"{API}/objects/{TENANT_A}/definitions", timeout=10).json()["definitions"]
        defn = next(d for d in defs if d["object_key"] == custom_object)
        f = next(x for x in defn["fields"] if x["code"] == "label_me")
        assert f["label"] == "中文标签"

    def test_patch_missing_field_rejected(self, custom_object):
        resp = S.patch(f"{API}/objects/{TENANT_A}/{custom_object}/fields/ghost",
                       json={"field_label": "x"}, timeout=10)
        assert resp.status_code == 400

    def test_patch_empty_body_rejected(self, custom_object):
        resp = S.patch(f"{API}/objects/{TENANT_A}/{custom_object}/fields/name",
                       json={}, timeout=10)
        assert resp.status_code == 400

    def test_patch_change_type_rejected(self, custom_object):
        """禁止物理改字段类型（数据收窄/丢失风险）。"""
        resp = S.patch(f"{API}/objects/{TENANT_A}/{custom_object}/fields/name",
                       json={"field_type": "int"}, timeout=10)
        assert resp.status_code == 400


# ── relations ────────────────────────────────────────────────────────────────
class TestRelations:
    def test_create_relation_ok(self, custom_object):
        resp = S.post(f"{API}/objects/{TENANT_A}/relations", json={
            "src_type": custom_object, "rel_type": "linked", "dst_type": "user",
            "edge_properties": [{"prop_key": "score", "prop_type": "int"}],
        }, timeout=10)
        assert resp.status_code == 200, resp.text
        assert resp.json()["rel_type"] == "linked"

    def test_create_builtin_relation_rejected(self):
        resp = S.post(f"{API}/objects/{TENANT_A}/relations", json={
            "src_type": "lead", "rel_type": "belongs_to", "dst_type": "user",
        }, timeout=10)
        assert resp.status_code == 400
        assert "内置" in resp.json()["detail"]

    def test_create_relation_unknown_object_rejected(self, custom_object):
        resp = S.post(f"{API}/objects/{TENANT_A}/relations", json={
            "src_type": custom_object, "rel_type": "linked", "dst_type": "ghostobj",
        }, timeout=10)
        assert resp.status_code == 400

    def test_create_duplicate_relation_rejected(self, custom_object):
        body = {"src_type": custom_object, "rel_type": "dupr", "dst_type": "user"}
        S.post(f"{API}/objects/{TENANT_A}/relations", json=body, timeout=10).raise_for_status()
        resp = S.post(f"{API}/objects/{TENANT_A}/relations", json=body, timeout=10)
        assert resp.status_code == 400

    def test_delete_custom_relation_ok(self, custom_object):
        S.post(f"{API}/objects/{TENANT_A}/relations", json={
            "src_type": custom_object, "rel_type": "delme", "dst_type": "user",
        }, timeout=10).raise_for_status()
        resp = S.delete(f"{API}/objects/{TENANT_A}/relations/{custom_object}/delme/user", timeout=10)
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_delete_builtin_relation_rejected(self):
        resp = S.delete(f"{API}/objects/{TENANT_A}/relations/lead/belongs_to/user", timeout=10)
        assert resp.status_code == 400
        assert "内置" in resp.json()["detail"]

    def test_delete_nonexistent_relation_rejected(self, custom_object):
        resp = S.delete(f"{API}/objects/{TENANT_A}/relations/{custom_object}/nope/user", timeout=10)
        assert resp.status_code == 400


# ── detail + 一跳关联 ─────────────────────────────────────────────────────────
@pytest.fixture
def seeded_lead():
    """用内置 upsert/relations 端点播种：用户 + 线索 + belongs_to 关系（独立测试租户）。"""
    lead_id = f"QA_{RUN}_LEAD"
    one_id = 990350777
    S.post(f"{API}/objects/upsert", json={
        "tenant_id": TENANT_A, "object": "user",
        "record": {"one_id": one_id, "phone": "13900000777", "tags": ["vip"]},
    }, timeout=10).raise_for_status()
    S.post(f"{API}/objects/upsert", json={
        "tenant_id": TENANT_A, "object": "lead",
        "record": {"lead_id": lead_id, "lead_name": "测试线索", "city": "上海",
                   "company_size": 600, "source": "qa", "stage": "new"},
    }, timeout=10).raise_for_status()
    S.post(f"{API}/objects/relations", json={
        "tenant_id": TENANT_A, "src_type": "lead", "src_id": lead_id,
        "rel_type": "belongs_to", "dst_type": "user", "dst_id": str(one_id),
    }, timeout=10).raise_for_status()
    return {"lead_id": lead_id, "one_id": one_id}


class TestDetail:
    def test_detail_existing(self, seeded_lead):
        resp = S.get(f"{API}/objects/{TENANT_A}/lead/{seeded_lead['lead_id']}", timeout=10)
        assert resp.status_code == 200
        body = resp.json()
        assert body["lead_id"] == seeded_lead["lead_id"]
        assert body["city"] == "上海"

    def test_detail_missing_returns_404(self):
        resp = S.get(f"{API}/objects/{TENANT_A}/lead/NO_SUCH_{RUN}", timeout=10)
        assert resp.status_code == 404

    def test_detail_unknown_object_rejected(self):
        resp = S.get(f"{API}/objects/{TENANT_A}/ghostobj/x", timeout=10)
        assert resp.status_code == 400


class TestObjectRelations:
    def test_relations_forward_edge(self, seeded_lead):
        resp = S.get(f"{API}/objects/{TENANT_A}/lead/{seeded_lead['lead_id']}/relations", timeout=10)
        assert resp.status_code == 200
        rels = resp.json()["relations"]
        assert "belongs_to" in rels
        edge = rels["belongs_to"][0]
        assert edge["object_key"] == "user"
        assert str(edge["id"]) == str(seeded_lead["one_id"])
        assert edge["direction"] == "forward"
        # 关联对象记录被回填
        assert edge["record"] is not None
        assert edge["record"]["one_id"] == seeded_lead["one_id"]

    def test_relations_reverse_edge(self, seeded_lead):
        """从 user 侧看，应能看到反向的 belongs_to 边。"""
        resp = S.get(f"{API}/objects/{TENANT_A}/user/{seeded_lead['one_id']}/relations", timeout=10)
        assert resp.status_code == 200
        rels = resp.json()["relations"]
        assert "belongs_to" in rels
        assert any(e["direction"] == "reverse" and e["object_key"] == "lead"
                   for e in rels["belongs_to"])

    def test_relations_empty_for_unrelated(self):
        resp = S.get(f"{API}/objects/{TENANT_A}/lead/NO_SUCH_{RUN}/relations", timeout=10)
        assert resp.status_code == 200
        assert resp.json()["relations"] == {}


# ── 多租户隔离 ────────────────────────────────────────────────────────────────
class TestTenantIsolation:
    def test_definitions_isolated(self, custom_object):
        """A 租户的自建对象不应出现在 B 租户的定义清单。"""
        resp = S.get(f"{API}/objects/{TENANT_B}/definitions", timeout=10)
        assert resp.status_code == 200
        keys = {d["object_key"] for d in resp.json()["definitions"]}
        assert custom_object not in keys

    def test_detail_isolated(self, seeded_lead):
        """A 租户播种的线索在 B 租户查不到（404）。"""
        resp = S.get(f"{API}/objects/{TENANT_B}/lead/{seeded_lead['lead_id']}", timeout=10)
        assert resp.status_code == 404

    def test_relations_isolated(self, seeded_lead):
        """A 租户的关系在 B 租户视角下为空。"""
        resp = S.get(f"{API}/objects/{TENANT_B}/lead/{seeded_lead['lead_id']}/relations", timeout=10)
        assert resp.status_code == 200
        assert resp.json()["relations"] == {}

    def test_add_field_isolated(self, custom_object):
        """A 租户的自建对象，在 B 租户下不可加字段（对象不存在）。"""
        resp = S.post(f"{API}/objects/{TENANT_B}/{custom_object}/fields",
                      json={"field_code": "x", "field_type": "str"}, timeout=10)
        assert resp.status_code == 400

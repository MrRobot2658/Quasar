"""09 · settings 模块测试 —— 工作区设置 / IAM（成员·角色·团队·邀请·API令牌·审计）

对标 Twilio Segment 的 Settings / IAM。覆盖正向、边界与多租户隔离。

前置：docker compose up -d mysql redis sql-engine && bash scripts/apply_migrations.sh
约定（与既有 test_module_*.py 保持一致）：
  - 服务不可达（连接失败）或 settings 路由未部署（404）时整体 skip，绝不 fail。
  - 用独立测试租户 + 带唯一后缀的数据，避免污染演示租户 1001/1002，且可幂等重跑。
  - IAM 各表仅以 tenant_id 隔离、不外键 tenants，故可直接用虚构测试租户号。
  - 工作区 GET/PATCH 依赖 tenants 表真实行：GET 用演示租户只读探测，PATCH 用
    临时 MySQL 种子租户（用完即删），MySQL 不可达时仅 skip 该用例。
  - 只读/只加，不触碰既有测试与演示数据。
"""

import os
import uuid

import pytest
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API = "http://localhost:8002"
# 绕过本机 http_proxy，避免 localhost 被代理（与既有测试一致）
S = requests.Session()
S.trust_env = False
_retry = Retry(total=3, connect=3, read=3, backoff_factor=0.2,
               allowed_methods=None, status_forcelist=[])
S.mount("http://", HTTPAdapter(max_retries=_retry))

# 独立测试租户，远离演示租户 1001/1002
TENANT_A = 990901
TENANT_B = 990902
# 工作区 PATCH 用的临时种子租户（仅经 MySQL 注入，用完删除）
TENANT_SEED = 990900

# 每次运行的唯一后缀，保证可重跑且互不串扰
RUN = uuid.uuid4().hex[:8]


def _settings_ready() -> bool:
    """settings 路由是否可用：连接失败或非 200（未部署）均视为不可用。"""
    try:
        r = S.get(f"{API}/iam/roles", params={"tenant_id": TENANT_A}, timeout=5)
    except requests.RequestException:
        return False
    return r.status_code == 200


pytestmark = pytest.mark.skipif(
    not _settings_ready(),
    reason="sql-engine settings 模块未就绪（服务未启动或路由未部署）",
)


# ── HTTP 小工具 ──────────────────────────────────────────────────────────────

def get(path: str, **params):
    return S.get(f"{API}{path}", params=params, timeout=15)


def post(path: str, body: dict | None = None, **params):
    return S.post(f"{API}{path}", json=body or {}, params=params, timeout=15)


def patch(path: str, body: dict, **params):
    return S.patch(f"{API}{path}", json=body, params=params, timeout=15)


def delete(path: str, **params):
    return S.delete(f"{API}{path}", params=params, timeout=15)


# ── 业务辅助：创建临时实体并返回 id ────────────────────────────────────────────

def _mk_user(tenant_id: int = TENANT_A, role_id: int | None = None) -> dict:
    email = f"user_{RUN}_{uuid.uuid4().hex[:6]}@test.local"
    r = post("/iam/users", {"tenant_id": tenant_id, "email": email,
                            "name": "测试成员", "role_id": role_id})
    assert r.status_code == 201, r.text
    return {**r.json(), "email": email}


def _mk_role(tenant_id: int = TENANT_A) -> dict:
    name = f"角色_{RUN}_{uuid.uuid4().hex[:6]}"
    r = post("/iam/roles", {"tenant_id": tenant_id, "name": name,
                            "scope": {"modules": ["segments"], "permissions": ["read"]}})
    assert r.status_code == 201, r.text
    return r.json()


def _mk_team(tenant_id: int = TENANT_A) -> dict:
    name = f"团队_{RUN}_{uuid.uuid4().hex[:6]}"
    r = post("/iam/teams", {"tenant_id": tenant_id, "name": name, "description": "测试团队"})
    assert r.status_code == 201, r.text
    return r.json()


# ════════════════════════════════════════════════════════════════════════
# 工作区 tenants（GET 只读探测 + PATCH 走临时种子租户）
# ════════════════════════════════════════════════════════════════════════

class TestWorkspace:
    def test_get_existing_tenant(self):
        """演示租户 1001 只读读取，返回规范化字段。"""
        r = get("/tenants/1001")
        assert r.status_code == 200
        data = r.json()
        assert data["id"] == 1001
        for k in ("name", "slug", "region", "plan", "tier"):
            assert k in data

    def test_get_missing_tenant_404(self):
        r = get("/tenants/99999991")
        assert r.status_code == 404

    def test_patch_missing_tenant_404(self):
        r = patch("/tenants/99999992", {"name": "x"})
        assert r.status_code == 404


# ── 需要 MySQL 的工作区 PATCH 用例（隔离在临时种子租户，用完即删）──────────────

def _mysql_conn():
    """按 conftest 同款默认连接 MySQL；不可达返回 None（→ skip 而非 fail）。"""
    try:
        import pymysql
    except ImportError:
        return None
    try:
        return pymysql.connect(
            host=os.getenv("TEST_MYSQL_HOST", "localhost"),
            port=int(os.getenv("TEST_MYSQL_PORT", "3308")),
            user=os.getenv("TEST_MYSQL_USER", "agenticdmp"),
            password=os.getenv("TEST_MYSQL_PASSWORD", "agenticdmp123"),
            database=os.getenv("TEST_MYSQL_DATABASE", "agenticdmp"),
            charset="utf8mb4",
            autocommit=True,
        )
    except Exception:
        return None


@pytest.fixture
def seeded_tenant():
    """临时注入一个种子租户，用完清理（含其 tenant_config / audit_log）。"""
    conn = _mysql_conn()
    if conn is None:
        pytest.skip("MySQL 不可达，跳过工作区 PATCH 落库用例")
    tid = TENANT_SEED
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM tenant_config WHERE tenant_id=%s", (tid,))
            cur.execute("DELETE FROM audit_log WHERE tenant_id=%s", (tid,))
            cur.execute("DELETE FROM tenants WHERE tenant_id=%s", (tid,))
            cur.execute(
                "INSERT INTO tenants (tenant_id, tenant_name, tier, kafka_topic) "
                "VALUES (%s, %s, 'standard', %s)",
                (tid, f"测试工作区_{RUN}", f"tenant-{tid}-events"),
            )
        yield tid
    finally:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM tenant_config WHERE tenant_id=%s", (tid,))
            cur.execute("DELETE FROM audit_log WHERE tenant_id=%s", (tid,))
            cur.execute("DELETE FROM tenants WHERE tenant_id=%s", (tid,))
        conn.close()


class TestWorkspacePatch:
    def test_patch_updates_name_region_plan(self, seeded_tenant):
        tid = seeded_tenant
        r = patch(f"/tenants/{tid}", {"name": f"改名_{RUN}", "region": "cn-north", "plan": "vip"})
        assert r.status_code == 200
        data = r.json()
        assert data["name"] == f"改名_{RUN}"
        assert data["region"] == "cn-north"
        assert data["plan"] == "vip"
        assert "updated_at" in data
        g = get(f"/tenants/{tid}").json()
        assert g["name"] == f"改名_{RUN}"
        assert g["region"] == "cn-north"
        assert g["plan"] == "vip"


# ════════════════════════════════════════════════════════════════════════
# 成员 users
# ════════════════════════════════════════════════════════════════════════

class TestUsers:
    def test_create_list_user(self):
        u = _mk_user()
        assert u["id"]
        assert u["status"] == "pending"

        r = get("/iam/users", tenant_id=TENANT_A)
        assert r.status_code == 200
        body = r.json()
        assert body["total"] >= 1
        row = {x["id"]: x for x in body["data"]}.get(u["id"])
        assert row is not None
        # 派生字段：role 单值 + teams 列表
        assert "role" in row and "teams" in row
        assert isinstance(row["teams"], list)

    def test_list_status_filter(self):
        u = _mk_user()
        patch(f"/iam/users/{u['id']}", {"status": "active"})
        r = get("/iam/users", tenant_id=TENANT_A, status="active")
        assert r.status_code == 200
        assert all(x["status"] == "active" for x in r.json()["data"])

    def test_update_name_and_status(self):
        u = _mk_user()
        r = patch(f"/iam/users/{u['id']}", {"name": "新名字", "status": "active"})
        assert r.status_code == 200
        assert r.json()["name"] == "新名字"
        assert r.json()["status"] == "active"

    def test_update_invalid_status_400(self):
        u = _mk_user()
        r = patch(f"/iam/users/{u['id']}", {"status": "banned"})
        assert r.status_code == 400

    def test_update_missing_user_404(self):
        r = patch("/iam/users/99999999", {"name": "x"})
        assert r.status_code == 404

    def test_delete_user_then_404(self):
        u = _mk_user()
        r = delete(f"/iam/users/{u['id']}")
        assert r.status_code == 200
        assert r.json()["ok"] is True
        # 二次删除 → 404
        assert delete(f"/iam/users/{u['id']}").status_code == 404

    def test_tenant_isolation_list_and_delete(self):
        """A 建的成员，B 列表看不到；B 跨租户删除 → 404（A 数据保留）。"""
        u = _mk_user(TENANT_A)
        r = get("/iam/users", tenant_id=TENANT_B)
        assert u["id"] not in {x["id"] for x in r.json()["data"]}
        # 跨租户删除被拒
        assert delete(f"/iam/users/{u['id']}", tenant_id=TENANT_B).status_code == 404
        # A 仍可见
        r = get("/iam/users", tenant_id=TENANT_A)
        assert u["id"] in {x["id"] for x in r.json()["data"]}


# ════════════════════════════════════════════════════════════════════════
# 角色 roles
# ════════════════════════════════════════════════════════════════════════

class TestRoles:
    def test_create_list_role(self):
        role = _mk_role()
        assert role["id"]
        assert isinstance(role["scope"], dict)

        r = get("/iam/roles", tenant_id=TENANT_A)
        assert r.status_code == 200
        row = {x["id"]: x for x in r.json()["data"]}.get(role["id"])
        assert row is not None
        assert "member_count" in row

    def test_update_role(self):
        role = _mk_role()
        r = patch(f"/iam/roles/{role['id']}",
                  {"name": f"改名角色_{RUN}", "scope": {"modules": ["all"]}})
        assert r.status_code == 200
        assert r.json()["name"] == f"改名角色_{RUN}"
        assert r.json()["scope"] == {"modules": ["all"]}

    def test_update_missing_role_404(self):
        assert patch("/iam/roles/99999999", {"name": "x"}).status_code == 404

    def test_delete_role_then_404(self):
        role = _mk_role()
        assert delete(f"/iam/roles/{role['id']}").status_code == 200
        assert delete(f"/iam/roles/{role['id']}").status_code == 404

    def test_member_count_reflects_assignment(self):
        """建用户时绑定角色，list_roles 的 member_count 应 >=1。"""
        role = _mk_role()
        _mk_user(TENANT_A, role_id=role["id"])
        r = get("/iam/roles", tenant_id=TENANT_A)
        row = {x["id"]: x for x in r.json()["data"]}[role["id"]]
        assert row["member_count"] >= 1

    def test_tenant_isolation(self):
        """A 的角色 B 看不到；B 跨租户删除 → 404。"""
        role = _mk_role(TENANT_A)
        r = get("/iam/roles", tenant_id=TENANT_B)
        assert role["id"] not in {x["id"] for x in r.json()["data"]}
        assert delete(f"/iam/roles/{role['id']}", tenant_id=TENANT_B).status_code == 404


# ════════════════════════════════════════════════════════════════════════
# 团队 teams + 成员关系
# ════════════════════════════════════════════════════════════════════════

class TestTeams:
    def test_create_list_team(self):
        team = _mk_team()
        assert team["id"]
        r = get("/iam/teams", tenant_id=TENANT_A)
        assert r.status_code == 200
        row = {x["id"]: x for x in r.json()["data"]}.get(team["id"])
        assert row is not None
        assert row["member_count"] == 0

    def test_add_and_remove_member(self):
        team = _mk_team()
        u = _mk_user()
        r = post(f"/iam/teams/{team['id']}/members", {"user_id": u["id"]})
        assert r.status_code == 200
        assert r.json()["ok"] is True

        # member_count +1
        teams = {x["id"]: x for x in get("/iam/teams", tenant_id=TENANT_A).json()["data"]}
        assert teams[team["id"]]["member_count"] == 1

        # 移除成员
        r = delete(f"/iam/teams/{team['id']}/members/{u['id']}")
        assert r.status_code == 200
        # 再次移除 → 404（已不在团队）
        assert delete(f"/iam/teams/{team['id']}/members/{u['id']}").status_code == 404

    def test_add_member_missing_team_404(self):
        u = _mk_user()
        r = post("/iam/teams/99999999/members", {"user_id": u["id"]})
        assert r.status_code == 404

    def test_add_member_idempotent(self):
        """重复添加同一成员幂等（INSERT IGNORE），member_count 仍为 1。"""
        team = _mk_team()
        u = _mk_user()
        post(f"/iam/teams/{team['id']}/members", {"user_id": u["id"]})
        post(f"/iam/teams/{team['id']}/members", {"user_id": u["id"]})
        teams = {x["id"]: x for x in get("/iam/teams", tenant_id=TENANT_A).json()["data"]}
        assert teams[team["id"]]["member_count"] == 1

    def test_tenant_isolation(self):
        team = _mk_team(TENANT_A)
        r = get("/iam/teams", tenant_id=TENANT_B)
        assert team["id"] not in {x["id"] for x in r.json()["data"]}


# ════════════════════════════════════════════════════════════════════════
# 邀请 invitations
# ════════════════════════════════════════════════════════════════════════

class TestInvitations:
    def _mk_invitation(self, tenant_id: int = TENANT_A, email: str | None = None) -> dict:
        role = _mk_role(tenant_id)
        email = email or f"invite_{RUN}_{uuid.uuid4().hex[:6]}@test.local"
        r = post("/iam/invitations", {"tenant_id": tenant_id, "email": email,
                                      "role_id": role["id"], "teams": []})
        assert r.status_code == 201, r.text
        return {**r.json(), "email": email, "role_id": role["id"]}

    def test_create_and_list(self):
        inv = self._mk_invitation()
        assert inv["token"]
        assert inv["email"] in inv["invitation_url"] or inv["token"] in inv["invitation_url"]
        assert "expires_at" in inv

        r = get("/iam/invitations", tenant_id=TENANT_A)
        assert r.status_code == 200
        assert inv["id"] in {x["id"] for x in r.json()["data"]}

    def test_list_status_filter_pending(self):
        self._mk_invitation()
        r = get("/iam/invitations", tenant_id=TENANT_A, status="pending")
        assert r.status_code == 200
        assert all(x["status"] == "pending" for x in r.json()["data"])

    def test_accept_invitation_activates_user(self):
        inv = self._mk_invitation()
        r = post(f"/iam/invitations/{inv['token']}/accept", {"name": "受邀者"})
        assert r.status_code == 200
        assert r.json()["status"] == "active"
        assert r.json()["user_id"]

        # 成员应已激活
        users = {x["email"]: x for x in get("/iam/users", tenant_id=TENANT_A).json()["data"]}
        assert users[inv["email"]]["status"] == "active"

    def test_accept_twice_400(self):
        inv = self._mk_invitation()
        assert post(f"/iam/invitations/{inv['token']}/accept", {}).status_code == 200
        # 二次接受 → 400（已处理）
        assert post(f"/iam/invitations/{inv['token']}/accept", {}).status_code == 400

    def test_accept_unknown_token_404(self):
        r = post(f"/iam/invitations/nonexistent_{RUN}/accept", {})
        assert r.status_code == 404

    def test_cancel_invitation_then_404(self):
        inv = self._mk_invitation()
        assert delete(f"/iam/invitations/{inv['id']}").status_code == 200
        assert delete(f"/iam/invitations/{inv['id']}").status_code == 404

    def test_tenant_isolation(self):
        inv = self._mk_invitation(TENANT_A)
        r = get("/iam/invitations", tenant_id=TENANT_B)
        assert inv["id"] not in {x["id"] for x in r.json()["data"]}
        # 跨租户取消 → 404
        assert delete(f"/iam/invitations/{inv['id']}", tenant_id=TENANT_B).status_code == 404


# ════════════════════════════════════════════════════════════════════════
# API 令牌 tokens
# ════════════════════════════════════════════════════════════════════════

class TestTokens:
    def _mk_token(self, tenant_id: int = TENANT_A) -> dict:
        label = f"令牌_{RUN}_{uuid.uuid4().hex[:6]}"
        r = post("/iam/tokens", {"tenant_id": tenant_id, "label": label,
                                 "scopes": ["read", "write"]})
        assert r.status_code == 201, r.text
        return r.json()

    def test_issue_token_returns_plaintext_once(self):
        tok = self._mk_token()
        assert tok["id"]
        assert tok["token_plaintext"].startswith("sk_")
        assert tok["prefix"]

    def test_list_tokens_hides_plaintext(self):
        tok = self._mk_token()
        r = get("/iam/tokens", tenant_id=TENANT_A)
        assert r.status_code == 200
        body = r.json()
        assert body["total"] >= 1
        row = {x["id"]: x for x in body["data"]}.get(tok["id"])
        assert row is not None
        # 明文绝不回显，仅前缀 + scopes 列表
        assert "token_plaintext" not in row
        assert "hash" not in row
        assert isinstance(row["scopes"], list)

    def test_revoke_token(self):
        tok = self._mk_token()
        r = delete(f"/iam/tokens/{tok['id']}")
        assert r.status_code == 200
        assert r.json()["ok"] is True
        assert r.json()["revoked_at"] is not None
        # 列表里该令牌已带 revoked_at
        row = {x["id"]: x for x in get("/iam/tokens", tenant_id=TENANT_A).json()["data"]}[tok["id"]]
        assert row["revoked_at"] is not None

    def test_revoke_missing_token_404(self):
        assert delete("/iam/tokens/99999999").status_code == 404

    def test_tenant_isolation(self):
        tok = self._mk_token(TENANT_A)
        r = get("/iam/tokens", tenant_id=TENANT_B)
        assert tok["id"] not in {x["id"] for x in r.json()["data"]}
        # 跨租户吊销 → 404
        assert delete(f"/iam/tokens/{tok['id']}", tenant_id=TENANT_B).status_code == 404


# ════════════════════════════════════════════════════════════════════════
# 审计 audit
# ════════════════════════════════════════════════════════════════════════

class TestAudit:
    def test_create_and_list(self):
        action = f"act_{RUN}_{uuid.uuid4().hex[:6]}"
        target = f"tgt_{RUN}"
        r = post("/iam/audit", {"tenant_id": TENANT_A, "actor": "pytest",
                                "action": action, "target": target,
                                "details": {"k": "中文值"}})
        assert r.status_code == 201
        assert r.json()["id"]

        r = get("/iam/audit", tenant_id=TENANT_A, action=action)
        assert r.status_code == 200
        assert r.json()["total"] >= 1
        assert all(x["action"] == action for x in r.json()["data"])

    def test_implicit_audit_on_iam_change(self):
        """创建角色等 IAM 操作会自动落审计，可按 actor=system 查到。"""
        _mk_role()
        r = get("/iam/audit", tenant_id=TENANT_A, actor="system")
        assert r.status_code == 200
        assert r.json()["total"] >= 1
        assert all(x["actor"] == "system" for x in r.json()["data"])

    def test_filter_by_target(self):
        target = f"tgt_uniq_{RUN}_{uuid.uuid4().hex[:6]}"
        post("/iam/audit", {"tenant_id": TENANT_A, "actor": "pytest",
                            "action": "x", "target": target})
        r = get("/iam/audit", tenant_id=TENANT_A, target=target)
        assert r.status_code == 200
        assert r.json()["total"] >= 1
        assert all(target in x["target"] for x in r.json()["data"])

    def test_tenant_isolation(self):
        action = f"iso_{RUN}_{uuid.uuid4().hex[:6]}"
        post("/iam/audit", {"tenant_id": TENANT_A, "actor": "pytest",
                            "action": action, "target": "t"})
        r = get("/iam/audit", tenant_id=TENANT_B, action=action)
        assert r.status_code == 200
        assert r.json()["total"] == 0

"""
用户画像实时 E2E 测试

场景：多渠道（微信 / 企业微信 / 表单）身份识别 + 行为汇总 → 统一用户画像

链路:
  渠道事件 → Kafka
          → OneID 实时合并（身份识别字段：openid / unionid / 企微ID / 表单ID / 手机号）
          → Redis 关系缓存
          → user_profile 属性 + 行为实时写入
          → Doris 宽表打宽 → 联合查询

运行:
  docker compose up -d --build
  bash scripts/run_profile_test.sh
"""

import json
import subprocess
import time
import uuid

import pymysql
import pytest
import requests

from conftest import API_BASE, MYSQL_CONFIG, REDIS_HOST, REDIS_PORT

RUN_ID = uuid.uuid4().hex[:8]

# 身份识别字段（跨渠道合并键）
IDENTITY_WECHAT = {
    "wechat_openid": f"wx_openid_{RUN_ID}",
    "wechat_unionid": f"wx_union_{RUN_ID}",
}
IDENTITY_WEWORK = {
    "wework_extid": f"ww_ext_{RUN_ID}",
}
IDENTITY_FORM = {
    "form_id": f"form_lead_{RUN_ID}",
    "phone": f"139{RUN_ID[:8]}",
}
SECONDARY_WECHAT_OPENID = f"wx_openid_{RUN_ID}_alt"

PROFILE_TENANT_PREMIUM = {
    "tenant_id": 1001,
    "topic": "tenant-1001-events",
    "identity": {**IDENTITY_WECHAT, **IDENTITY_WEWORK, **IDENTITY_FORM},
}

PROFILE_TENANT_STANDARD = {
    "tenant_id": 1002,
    "topic": "tenant-1002-events",
    "identity": {
        "wechat_openid": f"wx_std_{RUN_ID}",
        "form_id": f"form_std_{RUN_ID}",
        "phone": f"138{RUN_ID[:8]}",
    },
}

# 三渠道行为事件定义
WECHAT_BEHAVIORS = [
    ("微信-小程序浏览", {
        "channel_type": "wechat_openid",
        "identity_key": "wechat_openid",
        "event_type": "page_view",
        "link_keys": {},
        "properties": {"page": "product_detail", "source": "mini_program"},
    }),
    ("微信-授权登录", {
        "channel_type": "wechat_openid",
        "identity_key": "wechat_openid",
        "event_type": "login",
        "link_keys": {"wechat_unionid": "wechat_unionid"},
        "properties": {"login_method": "wechat_auth", "nickname": "张三"},
    }),
]

FORM_BEHAVIORS = [
    ("表单-活动留资", {
        "channel_type": "form_id",
        "identity_key": "form_id",
        "event_type": "form_submit",
        "link_keys": {"wechat_unionid": "wechat_unionid", "phone": "phone"},
        "properties": {
            "form_name": "618大促留资",
            "interest": "智能家居",
            "amount": 0,
            "order_count": 1,
        },
    }),
]

WEWORK_BEHAVIORS = [
    ("企微-添加好友", {
        "channel_type": "wework_extid",
        "identity_key": "wework_extid",
        "event_type": "add_friend",
        "link_keys": {"phone": "phone"},
        "properties": {"wework_tag": "高意向", "sales_owner": "小李"},
    }),
    ("企微-发送产品资料", {
        "channel_type": "wework_extid",
        "identity_key": "wework_extid",
        "event_type": "send_material",
        "link_keys": {"phone": "phone"},
        "properties": {"material": "产品白皮书", "amount": 15000, "order_count": 3},
    }),
]

IDENTITY_CONFLICT = [
    ("微信-备用小程序访问", {
        "channel_type": "wechat_openid",
        "channel_id": SECONDARY_WECHAT_OPENID,
        "event_type": "page_view",
        "link_keys": {},
        "properties": {"page": "campaign_landing"},
    }),
    ("微信-备用号绑定手机", {
        "channel_type": "wechat_openid",
        "channel_id": SECONDARY_WECHAT_OPENID,
        "event_type": "bind_phone",
        "link_keys": {"phone": "phone"},
        "properties": {"bind_source": "campaign_mini"},
    }),
]


def _send_profile_event(topic: str, tenant_id: int, event: dict):
    payload = json.dumps(event, ensure_ascii=False)
    proc = subprocess.run(
        [
            "docker", "exec", "-i", "agenticdmp-kafka",
            "kafka-console-producer",
            "--bootstrap-server", "kafka:29092",
            "--topic", topic,
            "--property", "parse.key=true",
            "--property", "key.separator=:",
        ],
        input=f"{tenant_id}:{payload}".encode(),
        capture_output=True,
        timeout=15,
    )
    if proc.returncode != 0:
        pytest.fail(f"Kafka 发送失败: {proc.stderr.decode()}")


def _resolve_event(tenant: dict, step: dict) -> dict:
    identity = tenant["identity"]
    channel_id = step.get("channel_id") or identity[step["identity_key"]]
    link_keys = {}
    for key, ref in step.get("link_keys", {}).items():
        link_keys[key] = identity.get(ref, ref)
    return {
        "tenant_id": tenant["tenant_id"],
        "channel_type": step["channel_type"],
        "channel_id": channel_id,
        "event_type": step["event_type"],
        "link_keys": link_keys,
        "properties": step.get("properties", {}),
    }


def _ingest_behaviors(tenant: dict, behavior_steps: list, interval: float = 0.8):
    for _, step in behavior_steps:
        event = _resolve_event(tenant, step)
        _send_profile_event(tenant["topic"], tenant["tenant_id"], event)
        time.sleep(interval)


def _wait_identity_mapping(tenant_id: int, channel_type: str, channel_id: str, timeout: float = 30.0) -> int:
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.get(f"{API_BASE}/mapping/{tenant_id}/{channel_type}/{channel_id}", timeout=5)
        if resp.status_code == 200:
            return resp.json()["one_id"]
        time.sleep(0.5)
    pytest.fail(f"身份映射超时: {tenant_id}/{channel_type}/{channel_id}")


def _assert_single_one_id(tenant_id: int, identity: dict[str, str]) -> int:
    one_ids = {_wait_identity_mapping(tenant_id, k, v) for k, v in identity.items()}
    assert len(one_ids) == 1, f"多渠道身份未合并到同一用户: {one_ids}"
    return one_ids.pop()


def _load_profile_props(tenant_id: int, one_id: int) -> dict:
    resp = requests.get(f"{API_BASE}/profile/{tenant_id}/{one_id}")
    assert resp.status_code == 200
    props = resp.json().get("properties") or {}
    if isinstance(props, str):
        props = json.loads(props)
    return props


def _behavior_types(props: dict) -> list[str]:
    return [b["event_type"] for b in props.get("behaviors", []) if isinstance(b, dict)]


class TestUserProfileRealtime:
    """用户画像：多渠道身份合并 + 行为实时汇总"""

    def test_profile_pipeline_health(self, services_ready):
        resp = requests.get(f"{API_BASE}/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_wechat_identity_and_behavior_updates_profile(self, services_ready):
        """微信渠道：身份字段创建/关联，浏览与登录行为实时写入画像"""
        tenant = PROFILE_TENANT_PREMIUM
        tid, identity = tenant["tenant_id"], tenant["identity"]

        _ingest_behaviors(tenant, WECHAT_BEHAVIORS)

        one_id = _wait_identity_mapping(tid, "wechat_openid", identity["wechat_openid"])
        assert _wait_identity_mapping(tid, "wechat_unionid", identity["wechat_unionid"]) == one_id

        props = _load_profile_props(tid, one_id)
        assert props.get("nickname") == "张三"
        assert props.get("last_behavior") == "login"
        assert props.get("last_channel") == "wechat_openid"
        assert "page_view" in _behavior_types(props)
        assert "login" in _behavior_types(props)

    def test_form_identity_links_wechat_and_behavior_updates_profile(self, services_ready):
        """表单渠道：留资身份(表单ID+手机号)关联微信，提交行为实时入画像"""
        tenant = PROFILE_TENANT_PREMIUM
        tid, identity = tenant["tenant_id"], tenant["identity"]

        _ingest_behaviors(tenant, WECHAT_BEHAVIORS)
        _ingest_behaviors(tenant, FORM_BEHAVIORS)

        one_id = _assert_single_one_id(tid, {
            "wechat_openid": identity["wechat_openid"],
            "form_id": identity["form_id"],
            "phone": identity["phone"],
        })

        props = _load_profile_props(tid, one_id)
        assert props.get("form_name") == "618大促留资"
        assert props.get("interest") == "智能家居"
        assert props.get("last_behavior") == "form_submit"
        assert props.get("last_channel") == "form_id"
        assert "form_submit" in _behavior_types(props)

    def test_wework_identity_links_form_and_behavior_updates_profile(self, services_ready):
        """企业微信渠道：企微ID 通过手机号关联表单用户，好友/触达行为实时入画像"""
        tenant = PROFILE_TENANT_PREMIUM
        tid, identity = tenant["tenant_id"], tenant["identity"]

        _ingest_behaviors(tenant, WECHAT_BEHAVIORS + FORM_BEHAVIORS + WEWORK_BEHAVIORS)

        one_id = _assert_single_one_id(tid, {
            "wechat_openid": identity["wechat_openid"],
            "form_id": identity["form_id"],
            "wework_extid": identity["wework_extid"],
            "phone": identity["phone"],
        })

        props = _load_profile_props(tid, one_id)
        assert props.get("wework_tag") == "高意向"
        assert props.get("material") == "产品白皮书"
        assert props.get("last_behavior") == "send_material"
        assert props.get("last_channel") == "wework_extid"
        behaviors = _behavior_types(props)
        assert "add_friend" in behaviors
        assert "send_material" in behaviors

    def test_multi_channel_identity_merge_and_profile_wide_table(self, services_ready):
        """三渠道身份汇总 + 行为聚合 + Redis/MySQL/Doris 宽表联合查询"""
        tenant = PROFILE_TENANT_PREMIUM
        tid, identity = tenant["tenant_id"], tenant["identity"]
        all_steps = WECHAT_BEHAVIORS + FORM_BEHAVIORS + WEWORK_BEHAVIORS + IDENTITY_CONFLICT

        for _, step in all_steps:
            _send_profile_event(tenant["topic"], tid, _resolve_event(tenant, step))
            time.sleep(0.8)

        one_id = _assert_single_one_id(tid, identity)

        # 备用微信 openid 身份冲突合并
        deadline = time.time() + 20
        while time.time() < deadline:
            resp = requests.get(
                f"{API_BASE}/mapping/{tid}/wechat_openid/{SECONDARY_WECHAT_OPENID}", timeout=5
            )
            if resp.status_code == 200 and resp.json()["one_id"] == one_id:
                break
            time.sleep(0.5)
        else:
            pytest.fail("备用微信身份未合并到主用户")

        # Redis 身份关系
        import redis as redis_lib
        r = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        for ctype, cid in list(identity.items()) + [("wechat_openid", SECONDARY_WECHAT_OPENID)]:
            assert r.get(f"channel:{tid}:{ctype}:{cid}") == str(one_id)

        # 画像属性与行为汇总
        props = _load_profile_props(tid, one_id)
        assert props.get("amount") == 15000
        assert len(props.get("behaviors", [])) >= 6
        assert set(_behavior_types(props)) >= {
            "page_view", "login", "form_submit", "add_friend", "send_material", "bind_phone",
        }

        # Doris 宽表：三渠道身份字段
        resp = requests.get(f"{API_BASE}/wide/{tid}/{one_id}")
        assert resp.status_code == 200
        wide = resp.json()
        assert wide["wechat_unionid"] == identity["wechat_unionid"]
        assert wide["wework_extid"] == identity["wework_extid"]
        assert wide["form_id"] == identity["form_id"]
        assert wide["phone"] == identity["phone"]
        assert wide["channel_count"] >= 5

        tags = wide["tags"]
        if isinstance(tags, str):
            tags = json.loads(tags)
        assert "high_value" in tags

        # 宽表联合查询：表单身份反查用户
        resp = requests.get(
            f"{API_BASE}/wide/query/{tid}",
            params={"channel_type": "form_id", "channel_id": identity["form_id"]},
        )
        assert resp.status_code == 200
        assert resp.json()["one_id"] == one_id

    def test_standard_tenant_wechat_and_form_profile(self, services_ready):
        """标准租户：微信 + 表单双渠道画像"""
        tenant = PROFILE_TENANT_STANDARD
        tid, identity = tenant["tenant_id"], tenant["identity"]

        steps = [
            ("微信-H5访问", {
                "channel_type": "wechat_openid",
                "identity_key": "wechat_openid",
                "event_type": "page_view",
                "link_keys": {},
                "properties": {"page": "landing"},
            }),
            ("表单-试用申请", {
                "channel_type": "form_id",
                "identity_key": "form_id",
                "event_type": "form_submit",
                "link_keys": {"wechat_openid": "wechat_openid", "phone": "phone"},
                "properties": {"form_name": "试用申请", "company": "测试科技"},
            }),
        ]
        _ingest_behaviors(tenant, steps)

        one_id = _assert_single_one_id(tid, identity)
        props = _load_profile_props(tid, one_id)
        assert props.get("company") == "测试科技"
        assert props.get("last_behavior") == "form_submit"

        resp = requests.get(f"{API_BASE}/wide/query/{tid}", params={
            "channel_type": "form_id", "channel_id": identity["form_id"],
        })
        assert resp.json()["wide"]["form_id"] == identity["form_id"]

    def test_offline_identity_links_new_wechat_visit(self, services_ready):
        """离线身份命中：历史 unionid 关联新的微信访问"""
        tid = 1001
        new_openid = f"wx_offline_{RUN_ID}"
        event = {
            "tenant_id": tid,
            "channel_type": "wechat_openid",
            "channel_id": new_openid,
            "event_type": "login",
            "link_keys": {"wechat_unionid": "union_abc123"},
            "properties": {"source": "offline_identity_test"},
        }
        _send_profile_event("tenant-1001-events", tid, event)

        one_id = _wait_identity_mapping(tid, "wechat_openid", new_openid)
        assert one_id == 100001

        props = _load_profile_props(tid, 100001)
        assert props.get("source") == "offline_identity_test"
        assert "login" in _behavior_types(props)

    def test_profile_wide_table_joint_query(self, services_ready, mysql_conn):
        """Doris 宽表与身份映射联合查询"""
        tenant = PROFILE_TENANT_PREMIUM
        tid, identity = tenant["tenant_id"], tenant["identity"]

        _ingest_behaviors(tenant, WECHAT_BEHAVIORS + FORM_BEHAVIORS)

        one_id = _wait_identity_mapping(tid, "form_id", identity["form_id"])

        with mysql_conn.cursor() as cur:
            cur.execute(
                """
                SELECT w.one_id, w.form_id, w.wechat_unionid, w.phone,
                       w.properties, COUNT(m.channel_id) AS identity_count
                FROM doris_user_wide w
                JOIN doris_id_mapping m ON w.tenant_id = m.tenant_id AND w.one_id = m.one_id
                WHERE w.tenant_id = %s AND w.one_id = %s
                GROUP BY w.one_id, w.form_id, w.wechat_unionid, w.phone, w.properties
                """,
                (tid, one_id),
            )
            row = cur.fetchone()

        assert row is not None
        assert row["form_id"] == identity["form_id"]
        assert row["identity_count"] >= 3
        props = row["properties"]
        if isinstance(props, str):
            props = json.loads(props)
        assert "form_submit" in _behavior_types(props)

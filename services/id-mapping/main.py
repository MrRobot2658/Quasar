"""
实时 ID-Mapping 服务
模拟文档中 Flink Job 的核心逻辑：Kafka → Redis 热层 → MySQL 冷层 → 用户合并
"""

import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import pymysql
import redis
from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("id-mapping")

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "agenticdmp"),
    "password": os.getenv("MYSQL_PASSWORD", "agenticdmp123"),
    "database": os.getenv("MYSQL_DATABASE", "agenticdmp"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": True,
}

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_TTL = int(os.getenv("REDIS_TTL_SECONDS", "2592000"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPICS = [t.strip() for t in os.getenv("KAFKA_TOPICS", "tenant-1001-events").split(",") if t.strip()]

CHANNEL_TYPES = {
    "wechat_openid", "wechat_unionid", "wework_extid", "form_id", "phone", "email", "device",
}


class UserEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: f"evt_{int(time.time() * 1000)}")
    tenant_id: int
    channel_type: str
    channel_id: str
    event_type: str = "page_view"
    event_time: str | None = None
    # 关联标识：用于跨渠道合并，如 union_id / phone / email
    link_keys: dict[str, str] = Field(default_factory=dict)
    properties: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class UserImportRecord(BaseModel):
    channel_type: str
    channel_id: str
    link_keys: dict[str, str] = Field(default_factory=dict)
    properties: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class UserImportRequest(BaseModel):
    tenant_id: int
    records: list[UserImportRecord] = Field(..., min_length=1, max_length=500)


class IdMappingService:
    def __init__(self):
        self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    @contextmanager
    def db(self):
        conn = pymysql.connect(**MYSQL_CONFIG)
        try:
            yield conn
        finally:
            conn.close()

    def _channel_key(self, tenant_id: int, channel_type: str, channel_id: str) -> str:
        return f"channel:{tenant_id}:{channel_type}:{channel_id}"

    def _uid_channels_key(self, tenant_id: int, one_id: int) -> str:
        return f"uid:{tenant_id}:{one_id}:channels"

    def get_one_id_from_redis(self, tenant_id: int, channel_type: str, channel_id: str) -> int | None:
        val = self.redis.get(self._channel_key(tenant_id, channel_type, channel_id))
        return int(val) if val else None

    def cache_mapping(self, tenant_id: int, channel_type: str, channel_id: str, one_id: int):
        pipe = self.redis.pipeline()
        pipe.setex(self._channel_key(tenant_id, channel_type, channel_id), REDIS_TTL, str(one_id))
        pipe.hset(self._uid_channels_key(tenant_id, one_id), channel_type, channel_id)
        pipe.expire(self._uid_channels_key(tenant_id, one_id), REDIS_TTL)
        pipe.execute()

    def query_one_id_from_mysql(self, tenant_id: int, channel_type: str, channel_id: str) -> int | None:
        with self.db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT one_id FROM id_mapping WHERE tenant_id=%s AND channel_type=%s AND channel_id=%s",
                    (tenant_id, channel_type, channel_id),
                )
                row = cur.fetchone()
                return row["one_id"] if row else None

    def query_one_id_by_link_keys(self, tenant_id: int, link_keys: dict[str, str]) -> int | None:
        for channel_type, channel_id in link_keys.items():
            if channel_type not in CHANNEL_TYPES or not channel_id:
                continue
            one_id = self.get_one_id_from_redis(tenant_id, channel_type, channel_id)
            if one_id:
                return one_id
            one_id = self.query_one_id_from_mysql(tenant_id, channel_type, channel_id)
            if one_id:
                self.cache_mapping(tenant_id, channel_type, channel_id, one_id)
                return one_id
        return None

    def generate_one_id(self, tenant_id: int) -> int:
        with self.db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO one_id_sequence (tenant_id, next_id) VALUES (%s, 100000) "
                    "ON DUPLICATE KEY UPDATE next_id = LAST_INSERT_ID(next_id + 1)",
                    (tenant_id,),
                )
                cur.execute("SELECT LAST_INSERT_ID() AS one_id")
                return cur.fetchone()["one_id"]

    def insert_mapping(self, tenant_id: int, channel_type: str, channel_id: str, one_id: int, source: str = "realtime"):
        with self.db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO id_mapping (tenant_id, channel_type, channel_id, one_id, source)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE one_id=VALUES(one_id), update_time=NOW(), source=VALUES(source)
                    """,
                    (tenant_id, channel_type, channel_id, one_id, source),
                )

    def merge_one_ids(self, tenant_id: int, from_id: int, to_id: int) -> int:
        """将 from_id 的所有渠道映射合并到 to_id（取较小 one_id 作为主 ID）"""
        primary = min(from_id, to_id)
        secondary = max(from_id, to_id)
        if primary == secondary:
            return primary

        with self.db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE id_mapping SET one_id=%s, source='merge', update_time=NOW() "
                    "WHERE tenant_id=%s AND one_id=%s",
                    (primary, tenant_id, secondary),
                )
                cur.execute(
                    "SELECT user_id, properties, tags, channel_type, channel_id "
                    "FROM user_profile WHERE tenant_id=%s AND user_id IN (%s, %s)",
                    (tenant_id, primary, secondary),
                )
                profiles = {row["user_id"]: row for row in cur.fetchall()}
                if profiles:
                    merged_props: dict = {}
                    merged_behaviors: list[dict] = []
                    merged_tags: list[str] = []
                    latest_channel = ("", "")
                    for uid in (primary, secondary):
                        row = profiles.get(uid)
                        if not row:
                            continue
                        props = row["properties"] or {}
                        if isinstance(props, str):
                            props = json.loads(props)
                        behaviors = props.pop("behaviors", [])
                        if isinstance(behaviors, list):
                            merged_behaviors.extend(behaviors)
                        merged_props.update(props)
                        tags = row["tags"] or []
                        if isinstance(tags, str):
                            tags = json.loads(tags)
                        merged_tags.extend(tags)
                        latest_channel = (row["channel_type"], row["channel_id"])
                    if merged_behaviors:
                        merged_props["behaviors"] = merged_behaviors[-20:]
                        last = merged_behaviors[-1]
                        merged_props["last_behavior"] = last.get("event_type")
                        merged_props["last_channel"] = last.get("channel_type")
                    merged_tags = self._compute_tags(merged_props) or list(dict.fromkeys(merged_tags))
                    cur.execute(
                        """
                        INSERT INTO user_profile (tenant_id, user_id, channel_type, channel_id, tags, properties)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            channel_type=VALUES(channel_type),
                            channel_id=VALUES(channel_id),
                            tags=VALUES(tags),
                            properties=VALUES(properties),
                            update_time=NOW()
                        """,
                        (
                            tenant_id, primary, latest_channel[0], latest_channel[1],
                            json.dumps(merged_tags, ensure_ascii=False),
                            json.dumps(merged_props, ensure_ascii=False),
                        ),
                    )
                    if secondary in profiles and secondary != primary:
                        cur.execute(
                            "DELETE FROM user_profile WHERE tenant_id=%s AND user_id=%s",
                            (tenant_id, secondary),
                        )
                cur.execute(
                    "SELECT channel_type, channel_id FROM id_mapping WHERE tenant_id=%s AND one_id=%s",
                    (tenant_id, primary),
                )
                mappings = cur.fetchall()

        for m in mappings:
            self.cache_mapping(tenant_id, m["channel_type"], m["channel_id"], primary)

        return primary

    def log_merge(self, tenant_id: int, event_id: str, action: str, one_id: int,
                  channel_type: str, channel_id: str, linked_one_id: int | None = None, detail: dict | None = None):
        with self.db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO merge_log (tenant_id, event_id, action, one_id, channel_type, channel_id, linked_one_id, detail)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (tenant_id, event_id, action, one_id, channel_type, channel_id, linked_one_id,
                     json.dumps(detail or {}, ensure_ascii=False)),
                )

    def sync_to_doris(self, tenant_id: int, one_id: int, event_time: str | None = None):
        """模拟 Flink → Doris：同步 id_mapping 并实时打宽表"""
        with self.db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO doris_id_mapping (tenant_id, channel_type, channel_id, one_id, source)
                    SELECT tenant_id, channel_type, channel_id, one_id, source
                    FROM id_mapping WHERE tenant_id=%s AND one_id=%s
                    ON DUPLICATE KEY UPDATE
                        one_id=VALUES(one_id), source=VALUES(source), update_time=NOW()
                    """,
                    (tenant_id, one_id),
                )
                cur.execute(
                    "SELECT channel_type, channel_id FROM id_mapping WHERE tenant_id=%s AND one_id=%s",
                    (tenant_id, one_id),
                )
                channels = {r["channel_type"]: r["channel_id"] for r in cur.fetchall()}
                cur.execute(
                    "SELECT tags, properties FROM user_profile WHERE tenant_id=%s AND user_id=%s",
                    (tenant_id, one_id),
                )
                profile = cur.fetchone() or {}
                tags = profile.get("tags") or "[]"
                properties = profile.get("properties") or "{}"
                if isinstance(tags, str):
                    tags = json.loads(tags)
                if isinstance(properties, str):
                    properties = json.loads(properties)

                cur.execute(
                    """
                    INSERT INTO doris_user_wide (
                        tenant_id, one_id,
                        wechat_openid, wechat_unionid, wework_extid, form_id, phone, email, device,
                        channel_count, tags, properties, last_event_time
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        wechat_openid=VALUES(wechat_openid),
                        wechat_unionid=VALUES(wechat_unionid),
                        wework_extid=VALUES(wework_extid),
                        form_id=VALUES(form_id),
                        phone=VALUES(phone),
                        email=VALUES(email),
                        device=VALUES(device),
                        channel_count=VALUES(channel_count),
                        tags=VALUES(tags),
                        properties=VALUES(properties),
                        last_event_time=COALESCE(VALUES(last_event_time), last_event_time),
                        update_time=NOW()
                    """,
                    (
                        tenant_id, one_id,
                        channels.get("wechat_openid"),
                        channels.get("wechat_unionid"),
                        channels.get("wework_extid"),
                        channels.get("form_id"),
                        channels.get("phone"),
                        channels.get("email"),
                        channels.get("device"),
                        len(channels),
                        json.dumps(tags, ensure_ascii=False),
                        json.dumps(properties, ensure_ascii=False),
                        event_time,
                    ),
                )

    def _compute_tags(self, properties: dict) -> list[str]:
        tags = []
        if properties.get("amount", 0) > 10000:
            tags.append("high_value")
        if properties.get("order_count", 0) > 10:
            tags.append("high_active")
        return tags

    def upsert_profile(
        self,
        tenant_id: int,
        one_id: int,
        channel_type: str,
        channel_id: str,
        properties: dict,
        event_type: str | None = None,
        explicit_tags: list[str] | None = None,
    ):
        with self.db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT properties FROM user_profile WHERE tenant_id=%s AND user_id=%s",
                    (tenant_id, one_id),
                )
                existing = cur.fetchone()
                existing_props = {}
                if existing and existing["properties"]:
                    existing_props = existing["properties"]
                    if isinstance(existing_props, str):
                        existing_props = json.loads(existing_props)
                merged_props = {**existing_props, **properties}
                if event_type:
                    behaviors = existing_props.get("behaviors", [])
                    if not isinstance(behaviors, list):
                        behaviors = []
                    behaviors.append({
                        "event_type": event_type,
                        "channel_type": channel_type,
                        "channel_id": channel_id,
                        "at": datetime.now().isoformat(),
                    })
                    merged_props["behaviors"] = behaviors[-20:]
                    merged_props["last_behavior"] = event_type
                    merged_props["last_channel"] = channel_type
                tags = self._compute_tags(merged_props)
                if explicit_tags:
                    for t in explicit_tags:
                        if t and t not in tags:
                            tags.append(t)

                cur.execute(
                    """
                    INSERT INTO user_profile (tenant_id, user_id, channel_type, channel_id, tags, properties)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        channel_type=VALUES(channel_type),
                        channel_id=VALUES(channel_id),
                        tags=VALUES(tags),
                        properties=VALUES(properties),
                        update_time=NOW()
                    """,
                    (tenant_id, one_id, channel_type, channel_id, json.dumps(tags), json.dumps(merged_props, ensure_ascii=False)),
                )

    def process_event(self, event: UserEvent) -> dict:
        tenant_id = event.tenant_id
        channel_type = event.channel_type
        channel_id = event.channel_id

        # 1. Redis 热层查询
        one_id = self.get_one_id_from_redis(tenant_id, channel_type, channel_id)
        action = "hit_cache"

        # 2. Redis miss → MySQL 冷层
        if one_id is None:
            one_id = self.query_one_id_from_mysql(tenant_id, channel_type, channel_id)
            if one_id:
                self.cache_mapping(tenant_id, channel_type, channel_id, one_id)
                action = "hit_mysql"

        # 3. 通过 link_keys 尝试跨渠道关联
        linked_one_id = None
        if event.link_keys:
            linked_one_id = self.query_one_id_by_link_keys(tenant_id, event.link_keys)

        if one_id and linked_one_id and one_id != linked_one_id:
            # 发现需合并的两个 OneID
            merged = self.merge_one_ids(tenant_id, one_id, linked_one_id)
            one_id = merged
            action = "merge"
            self.log_merge(tenant_id, event.event_id, "merge", one_id, channel_type, channel_id,
                           linked_one_id, {"link_keys": event.link_keys})
        elif one_id is None and linked_one_id:
            # 当前渠道新用户，但 link_keys 命中已有用户 → 关联
            one_id = linked_one_id
            self.insert_mapping(tenant_id, channel_type, channel_id, one_id, source="link")
            self.cache_mapping(tenant_id, channel_type, channel_id, one_id)
            action = "link"
            self.log_merge(tenant_id, event.event_id, "link", one_id, channel_type, channel_id,
                           linked_one_id, {"link_keys": event.link_keys})
        elif one_id is None:
            # 全新用户
            one_id = self.generate_one_id(tenant_id)
            self.insert_mapping(tenant_id, channel_type, channel_id, one_id, source="realtime")
            self.cache_mapping(tenant_id, channel_type, channel_id, one_id)
            action = "create"
            self.log_merge(tenant_id, event.event_id, "create", one_id, channel_type, channel_id)

        # 同步 link_keys 到映射表
        for lk_type, lk_id in event.link_keys.items():
            if lk_type in CHANNEL_TYPES and lk_id:
                self.insert_mapping(tenant_id, lk_type, lk_id, one_id, source="link")
                self.cache_mapping(tenant_id, lk_type, lk_id, one_id)

        # 更新用户画像（属性 + 行为实时汇总）
        self.upsert_profile(
            tenant_id, one_id, channel_type, channel_id,
            event.properties, event.event_type, explicit_tags=event.tags or None,
        )

        # 模拟 Flink → Doris 实时打宽
        self.sync_to_doris(tenant_id, one_id, event.event_time)

        return {
            "event_id": event.event_id,
            "tenant_id": tenant_id,
            "one_id": one_id,
            "channel_type": channel_type,
            "channel_id": channel_id,
            "action": action,
            "linked_one_id": linked_one_id,
            "processed_at": datetime.now().isoformat(),
        }


service = IdMappingService()

ID_MAPPING_TAGS = [
    {"name": "系统", "description": "健康检查"},
    {"name": "事件处理", "description": "实时用户事件 / Kafka 消费"},
    {"name": "身份映射", "description": "渠道 ID → OneID 查询"},
    {"name": "用户画像", "description": "画像与宽表查询"},
    {"name": "审计", "description": "合并操作日志"},
]

ROOT_PATH = os.getenv("ROOT_PATH", "")

app = FastAPI(
    title="ID-Mapping API",
    description="实时 ID-Mapping 服务 — Kafka → OneID 合并 → Redis/MySQL/Doris",
    version="1.1.0",
    openapi_tags=ID_MAPPING_TAGS,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    root_path=ROOT_PATH,
)


def kafka_consumer_loop():
    logger.info("Starting Kafka consumer, topics=%s", KAFKA_TOPICS)
    while True:
        try:
            consumer = KafkaConsumer(
                *KAFKA_TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="id-mapping-group",
                consumer_timeout_ms=1000,
            )
            logger.info("Kafka consumer connected")
            while True:
                for message in consumer:
                    try:
                        payload = message.value
                        # shared topic 事件可能在 value 里带 tenant_id
                        event = UserEvent(**payload)
                        result = service.process_event(event)
                        logger.info("Processed: %s", json.dumps(result, ensure_ascii=False))
                    except Exception as e:
                        logger.error("Failed to process message: %s, error=%s", message.value, e)
        except Exception as e:
            logger.warning("Kafka consumer error, retrying in 5s: %s", e)
            time.sleep(5)


@app.on_event("startup")
def startup():
    t = threading.Thread(target=kafka_consumer_loop, daemon=True)
    t.start()


@app.get("/health", tags=["系统"])
def health():
    return {"status": "ok", "topics": KAFKA_TOPICS}


@app.post("/events/process", tags=["事件处理"])
def process_event_api(event: UserEvent):
    """处理用户事件（模拟 Flink Job），完成 OneID 合并与画像更新"""
    return service.process_event(event)


@app.post("/users/import", tags=["事件处理"])
def import_users(body: UserImportRequest):
    """批量导入用户（离线渠道身份 → OneID 合并 → 画像写入）"""
    results = []
    errors = []
    for i, rec in enumerate(body.records):
        try:
            event = UserEvent(
                tenant_id=body.tenant_id,
                channel_type=rec.channel_type,
                channel_id=rec.channel_id,
                link_keys=rec.link_keys,
                properties=rec.properties,
                tags=rec.tags,
                event_type="import",
            )
            results.append(service.process_event(event))
        except Exception as e:
            errors.append({"index": i, "channel_id": rec.channel_id, "error": str(e)})
    return {
        "tenant_id": body.tenant_id,
        "imported": len(results),
        "failed": len(errors),
        "results": results,
        "errors": errors,
    }


@app.get("/mapping/{tenant_id}/{channel_type}/{channel_id}", tags=["身份映射"])
def get_mapping(tenant_id: int, channel_type: str, channel_id: str):
    one_id = service.get_one_id_from_redis(tenant_id, channel_type, channel_id)
    source = "redis"
    if one_id is None:
        one_id = service.query_one_id_from_mysql(tenant_id, channel_type, channel_id)
        source = "mysql"
    if one_id is None:
        raise HTTPException(status_code=404, detail="Mapping not found")
    return {"tenant_id": tenant_id, "channel_type": channel_type, "channel_id": channel_id, "one_id": one_id, "source": source}


@app.get("/profile/{tenant_id}/{one_id}", tags=["用户画像"])
def get_profile(tenant_id: int, one_id: int):
    with service.db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM user_profile WHERE tenant_id=%s AND user_id=%s",
                (tenant_id, one_id),
            )
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Profile not found")
    return row


@app.get("/wide/query/{tenant_id}", tags=["用户画像"])
def query_wide_by_channel(tenant_id: int, channel_type: str, channel_id: str):
    """通过任意渠道 ID 联合查询 Doris 宽表"""
    one_id = service.get_one_id_from_redis(tenant_id, channel_type, channel_id)
    if one_id is None:
        one_id = service.query_one_id_from_mysql(tenant_id, channel_type, channel_id)
    if one_id is None:
        raise HTTPException(status_code=404, detail="Mapping not found")
    with service.db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM doris_user_wide WHERE tenant_id=%s AND one_id=%s",
                (tenant_id, one_id),
            )
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Wide table record not found")
    return {"one_id": one_id, "wide": row}


@app.get("/wide/{tenant_id}/{one_id}", tags=["用户画像"])
def get_wide_table(tenant_id: int, one_id: int):
    """Doris 宽表联合查询（模拟）"""
    with service.db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM doris_user_wide WHERE tenant_id=%s AND one_id=%s",
                (tenant_id, one_id),
            )
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Wide table record not found")
    return row


@app.get("/merge-log/{tenant_id}", tags=["审计"])
def get_merge_log(tenant_id: int, limit: int = 20):
    with service.db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM merge_log WHERE tenant_id=%s ORDER BY created_at DESC LIMIT %s",
                (tenant_id, limit),
            )
            rows = cur.fetchall()
    return rows


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

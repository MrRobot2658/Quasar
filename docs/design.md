# 多租户用户画像实时链路设计

> 版本 v1.1 | 场景：微信 / 企业微信 / 表单 → 身份合并 → 行为汇总 → 用户画像 → Doris 宽表

## 1. 方案总览

### 1.1 核心目标

1. 租户数据物理隔离 + 逻辑隔离
2. 按租户数据量水平伸缩（小租户共享、大租户独占）
3. 租户内多渠道实时 ID 打通（OneID）
4. 用户画像属性 + 行为实时更新，秒级可查

### 1.2 总体架构

```
┌─────────────────────────────────────────────────────────────┐
│                    StreamPark 管控面                          │
│  id-mapping-1001  │  profile-1001  │  wide-1001  │ shared  │
└────────┬──────────────────┬─────────────────┬──────────────┘
         │                  │                 │
    ┌────▼────┐        ┌────▼────┐       ┌────▼────┐
    │ Kafka   │        │ enriched│       │ Kafka   │
    │ events  │───────▶│ events  │──────▶│ sink    │
    └────┬────┘        └────┬────┘       └────┬────┘
         │                  │                 │
    ┌────▼──────────────────▼─────────────────▼────┐
    │  Flink: ID-Mapping → 画像聚合 → 宽表打宽        │
    └────┬──────────────────┬─────────────────┬────┘
         │                  │                 │
    ┌────▼────┐        ┌────▼────┐       ┌────▼──────────┐
    │  Redis  │        │  MySQL  │       │  Apache Doris │
    │  热层   │        │  业务库  │       │  OLAP + 宽表  │
    └─────────┘        └─────────┘       └───────────────┘
```

### 1.3 本地开发环境映射

| 生产组件 | 本地实现 | 路径 |
|---------|---------|------|
| Kafka | `agenticdmp-kafka` | `docker-compose.yml` |
| Flink Job | Python id-mapping 服务 | `services/id-mapping/main.py` |
| Redis | `agenticdmp-redis` | 端口 6381 |
| MySQL | `agenticdmp-mysql` | `sql/init.sql` |
| Doris | MySQL 模拟表 `doris_*` | `sql/init.sql` |

Flink 生产 Job 模板见 [`docs/flink/`](./flink/README.md)。

---

## 2. Kafka Topic 设计

### 2.1 Topic 分级

| 租户类型 | Topic | 分区(生产) | 副本 | Message Key |
|---------|-------|-----------|------|-------------|
| premium 大租户 | `tenant-{id}-events` | 16 | 3 | `tenant_id` 或 `channel_id` |
| standard 小租户 | `tenant-{id}-events` | 2~4 | 2 | `tenant_id` |
| 微租户共享 | `shared-tenant-events` | 8 | 2 | `tenant_id`（body 也带 tenant_id） |

**本地环境：**

| Topic | 分区 | 租户 |
|-------|------|------|
| `tenant-1001-events` | 4 | 1001 品牌A |
| `tenant-1002-events` | 2 | 1002 品牌B |
| `shared-tenant-events` | 4 | 1003+ |

### 2.2 创建命令

```bash
# 大租户
kafka-topics.sh --create \
  --topic tenant-1001-events \
  --partitions 16 --replication-factor 3 \
  --config retention.ms=604800000 \
  --config compression.type=lz4

# 共享 Topic
kafka-topics.sh --create \
  --topic shared-tenant-events \
  --partitions 8 --replication-factor 2
```

### 2.3 下游 Topic（Flink 输出）

| Topic | 生产者 | 消费者 | 说明 |
|-------|--------|--------|------|
| `enriched-{tenant_id}-events` | Job-1 ID-Mapping | Job-2 画像 / Job-3 宽表 | 带 one_id 的富化事件 |

### 2.4 消息 Schema（UserEvent）

```json
{
  "event_id": "evt_1718123456789",
  "tenant_id": 1001,
  "channel_type": "form_id",
  "channel_id": "form_lead_abc123",
  "event_type": "form_submit",
  "event_time": "2026-06-12T10:00:00",
  "link_keys": {
    "wechat_unionid": "wx_union_abc",
    "phone": "13900001111"
  },
  "properties": {
    "form_name": "618大促留资",
    "interest": "智能家居",
    "amount": 0,
    "order_count": 1
  }
}
```

### 2.5 channel_type（身份识别字段）

| 值 | 渠道 | 说明 |
|----|------|------|
| `wechat_openid` | 微信 | 小程序/H5 openid |
| `wechat_unionid` | 微信 | 跨应用统一身份 |
| `wework_extid` | 企业微信 | 外部联系人 ID |
| `form_id` | 表单 | 留资记录 ID |
| `phone` | 通用 | 强关联键 |
| `email` | 通用 | 邮箱 |
| `device` | 通用 | 设备 ID |

### 2.6 event_type（行为类型）

| 渠道 | event_type |
|------|-----------|
| 微信 | `page_view` / `login` / `bind_phone` |
| 表单 | `form_submit` / `form_update` |
| 企微 | `add_friend` / `send_material` |

### 2.7 Producer 规范

```java
// 大租户独立 Topic
producer.send(new ProducerRecord<>(
    "tenant-1001-events",
    String.valueOf(tenantId).getBytes(),
    eventJson
));

// 共享 Topic：key 必须为 tenant_id，保证同租户有序
producer.send(new ProducerRecord<>(
    "shared-tenant-events",
    String.valueOf(tenantId).getBytes(),
    eventJson
));
```

---

## 3. Flink 任务设计

拆为 3 个 Job，由 StreamPark 按租户部署。模板代码见 `docs/flink/`。

### 3.1 Job-1：实时 ID-Mapping（DataStream）

**职责：** 身份识别字段实时合并，输出带 `one_id` 的富化事件。

```
Kafka(user-events)
  → keyBy(tenant_id + channel_type + channel_id)
  → IdMappingFunction
      ├─ Redis 查 channel → one_id
      ├─ miss → Doris/MySQL id_mapping
      ├─ link_keys 跨渠道关联
      ├─ create / link / merge
      ├─ 写 Redis + Doris id_mapping
      └─ 写 MySQL（可选）
  → Kafka(enriched-events)
```

| 配置项 | 大租户 1001 | 小租户 1002 | 共享 Job |
|--------|------------|-------------|---------|
| 并行度 | 16 | 2~4 | 8 |
| State | RocksDB | RocksDB | RocksDB |
| Checkpoint | 60s | 60s | 60s |

> Job-1 含 merge 状态机，**必须用 DataStream**，不适合纯 Flink SQL。

模板：`docs/flink/src/main/java/com/agenticdmp/flink/IdMappingJob.java`

### 3.2 Job-2：用户画像实时聚合（Flink SQL）

**职责：** 属性 merge + 行为 append + 标签计算 → `user_profile`。

```
Kafka(enriched-events)
  → GROUP BY tenant_id, one_id
  → 聚合 properties / tags
  → Doris user_profile UNIQUE KEY Upsert
  → Redis 热点画像缓存（Async Sink）
```

模板：`docs/flink/sql/job-02-profile-aggregation.sql`

### 3.3 Job-3：Doris 宽表实时打宽（Flink SQL）

**职责：** 多渠道身份列展开 + 画像快照 → `user_wide`。

```
Kafka(enriched-events) 或 Doris id_mapping CDC
  → JOIN user_profile
  → 列展开 wechat_openid / wework_extid / form_id / phone ...
  → Doris user_wide UNIQUE KEY Upsert
```

模板：`docs/flink/sql/job-03-wide-table.sql`

### 3.4 StreamPark 部署矩阵

| Job 名 | 类型 | Source | Sink |
|--------|------|--------|------|
| `id-mapping-1001` | DataStream | `tenant-1001-events` | Redis + Doris + `enriched-1001-events` |
| `profile-1001` | Flink SQL | `enriched-1001-events` | Doris `user_profile` |
| `wide-1001` | Flink SQL | `enriched-1001-events` | Doris `user_wide` |
| `id-mapping-shared` | DataStream | `shared-tenant-events` | 同上，按 tenant_id 过滤 |

---

## 4. MySQL 表设计（业务冷层）

MySQL 承担：发号器、离线映射导入、画像备份、合并审计。

### 4.1 tenants

```sql
CREATE TABLE tenants (
    tenant_id       BIGINT PRIMARY KEY,
    tenant_name     VARCHAR(128) NOT NULL,
    tier            ENUM('premium', 'standard') NOT NULL DEFAULT 'standard',
    kafka_topic     VARCHAR(128) NOT NULL,
    doris_db        VARCHAR(64) COMMENT 'premium→tenant_{id}',
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;
```

### 4.2 id_mapping

```sql
CREATE TABLE id_mapping (
    tenant_id       BIGINT NOT NULL,
    channel_type    VARCHAR(32) NOT NULL
        COMMENT 'wechat_openid/wechat_unionid/wework_extid/form_id/phone/email/device',
    channel_id      VARCHAR(256) NOT NULL,
    one_id          BIGINT NOT NULL,
    confidence      DOUBLE DEFAULT 1.0,
    source          VARCHAR(32) DEFAULT 'realtime'
        COMMENT 'offline/login/link/merge/realtime',
    create_time     DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, channel_type, channel_id),
    INDEX idx_one_id (tenant_id, one_id)
) ENGINE=InnoDB COMMENT='渠道身份 → OneID';
```

### 4.3 one_id_sequence

```sql
CREATE TABLE one_id_sequence (
    tenant_id       BIGINT PRIMARY KEY,
    next_id         BIGINT NOT NULL DEFAULT 100000
) ENGINE=InnoDB;

-- 原子发号
INSERT INTO one_id_sequence (tenant_id, next_id) VALUES (?, 100000)
ON DUPLICATE KEY UPDATE next_id = LAST_INSERT_ID(next_id + 1);
SELECT LAST_INSERT_ID();
```

### 4.4 user_profile

```sql
CREATE TABLE user_profile (
    tenant_id       BIGINT NOT NULL,
    user_id         BIGINT NOT NULL COMMENT 'OneID',
    channel_type    VARCHAR(32),
    channel_id      VARCHAR(128),
    tags            JSON,
    properties      JSON COMMENT '属性+行为，含 behaviors[]',
    update_time     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, user_id),
    INDEX idx_update_time (update_time)
) ENGINE=InnoDB COMMENT='用户画像';
```

**properties 示例：**

```json
{
  "nickname": "张三",
  "form_name": "618大促留资",
  "amount": 15000,
  "last_behavior": "send_material",
  "last_channel": "wework_extid",
  "behaviors": [
    {"event_type": "page_view",   "channel_type": "wechat_openid", "at": "2026-06-12T10:00:00"},
    {"event_type": "form_submit", "channel_type": "form_id",       "at": "2026-06-12T10:01:00"},
    {"event_type": "add_friend",  "channel_type": "wework_extid",  "at": "2026-06-12T10:05:00"}
  ]
}
```

### 4.5 merge_log

```sql
CREATE TABLE merge_log (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id       BIGINT NOT NULL,
    event_id        VARCHAR(64),
    action          VARCHAR(32) NOT NULL COMMENT 'create/link/merge',
    one_id          BIGINT NOT NULL,
    channel_type    VARCHAR(32),
    channel_id      VARCHAR(256),
    linked_one_id   BIGINT,
    detail          JSON,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tenant_time (tenant_id, created_at)
) ENGINE=InnoDB COMMENT='身份合并审计';
```

---

## 5. Doris 表设计

### 5.1 隔离策略

| 租户类型 | 策略 |
|---------|------|
| premium | 独立库 `tenant_{id}`，BE Resource Tag 物理隔离 |
| standard | 共享库 `tenant_shared`，`tenant_id` 分区逻辑隔离 |

### 5.2 id_mapping

```sql
CREATE TABLE id_mapping (
    tenant_id       BIGINT       NOT NULL,
    channel_type    VARCHAR(32)  NOT NULL,
    channel_id      VARCHAR(256) NOT NULL,
    one_id          BIGINT       NOT NULL,
    confidence      DOUBLE       DEFAULT 1.0,
    source          VARCHAR(32)  DEFAULT 'realtime',
    create_time     DATETIME,
    update_time     DATETIME
)
UNIQUE KEY(tenant_id, channel_type, channel_id)
DISTRIBUTED BY HASH(tenant_id, channel_type, channel_id) BUCKETS 16
PROPERTIES ("enable_unique_key_merge_on_write" = "true");

ALTER TABLE id_mapping ADD INDEX idx_one_id (one_id) USING INVERTED;
```

### 5.3 user_profile

**大租户：**

```sql
CREATE TABLE tenant_1001.user_profile (
    user_id         BIGINT       NOT NULL,
    channel_type    VARCHAR(32),
    channel_id      VARCHAR(128),
    tags            BITMAP,
    properties      JSON,
    update_time     DATETIME
)
UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 32;
```

**小租户共享：**

```sql
CREATE TABLE tenant_shared.user_profile (
    tenant_id       BIGINT       NOT NULL,
    user_id         BIGINT       NOT NULL,
    channel_type    VARCHAR(32),
    channel_id      VARCHAR(128),
    tags            BITMAP,
    properties      JSON,
    update_time     DATETIME
)
UNIQUE KEY(tenant_id, user_id)
PARTITION BY LIST(tenant_id) (
    PARTITION p_1002 VALUES IN ("1002"),
    PARTITION p_1003 VALUES IN ("1003")
)
DISTRIBUTED BY HASH(tenant_id, user_id) BUCKETS 16;
```

### 5.4 user_wide（实时打宽）

```sql
CREATE TABLE tenant_1001.user_wide (
    one_id              BIGINT       NOT NULL,

    -- 身份识别字段（列展开）
    wechat_openid       VARCHAR(256),
    wechat_unionid      VARCHAR(256),
    wework_extid        VARCHAR(256),
    form_id             VARCHAR(256),
    phone               VARCHAR(256),
    email               VARCHAR(256),
    device              VARCHAR(256),

    -- 画像汇总
    channel_count       INT          DEFAULT 0,
    tags                BITMAP,
    properties          JSON,

    last_event_time     DATETIME,
    update_time         DATETIME
)
UNIQUE KEY(one_id)
DISTRIBUTED BY HASH(one_id) BUCKETS 32
PROPERTIES ("enable_unique_key_merge_on_write" = "true");

ALTER TABLE user_wide ADD INDEX idx_phone    (phone)        USING INVERTED;
ALTER TABLE user_wide ADD INDEX idx_form_id  (form_id)      USING INVERTED;
ALTER TABLE user_wide ADD INDEX idx_wework   (wework_extid) USING INVERTED;
```

### 5.5 典型查询

```sql
-- 表单留资反查全渠道身份
SELECT one_id, wechat_openid, wework_extid, phone, properties
FROM tenant_1001.user_wide WHERE form_id = 'form_lead_abc123';

-- 高价值用户圈选
SELECT one_id, phone, properties
FROM tenant_1001.user_wide
WHERE BITMAP_CONTAINS(tags, BITMAP_FROM_STRING('high_value'));

-- 宽表 + 映射明细 JOIN
SELECT w.*, m.channel_type, m.source
FROM tenant_1001.user_wide w
JOIN id_mapping m ON w.one_id = m.one_id
WHERE m.tenant_id = 1001;
```

---

## 6. Redis 热层设计

```
# 渠道 → OneID
SET channel:{tenant_id}:{channel_type}:{channel_id} {one_id}  EX 2592000

# OneID → 所有渠道（反向查询）
HSET uid:{tenant_id}:{one_id}:channels  wechat_openid  oXxx...
HSET uid:{tenant_id}:{one_id}:channels  form_id        form_lead_abc
EXPIRE uid:{tenant_id}:{one_id}:channels 2592000

# 热点画像（可选）
SET profile:{tenant_id}:{one_id} {json}  EX 3600
```

---

## 7. 三层存储职责

| 存储 | 角色 | 典型查询延迟 |
|------|------|-------------|
| Redis | channel↔one_id、热点画像 | \< 5ms |
| MySQL | 发号、离线导入、审计 | 10~50ms |
| Doris | 全量映射、画像、宽表、圈选 | 点查 \< 5ms，圈选 1~3s |

---

## 8. SQL Engine 查询层（与 Doris 解耦）

```
业务应用 / BI
      │
      ▼
┌─────────────┐     模板名 + 参数      ┌──────────────┐
│ SQL Engine  │ ─────────────────────▶ │ OlapExecutor │
│  :8002      │     拼装 SQL           │  (可切换)     │
└─────────────┘                        └──────┬───────┘
                                              │
                         ┌────────────────────┼────────────────────┐
                         ▼                    ▼                    ▼
                   MySQL 模拟           Doris FE:9030        其他 OLAP
                   (本地开发)            (生产环境)
```

- 查询模板：`services/sql-engine/templates/olap_queries.yaml`
- 切换后端：`OLAP_BACKEND=mysql|doris`
- API：`POST /query/{template_name}` + `{"params": {...}}`

## 9. Docker 规模模拟

```bash
bash scripts/scale-up.sh dev      # <1万
bash scripts/scale-up.sh medium   # 1000万
bash scripts/scale-up.sh large    # 1亿
bash scripts/scale-up.sh xlarge   # 2亿
```

详见 [scale-comparison.md](./scale-comparison.md)。

## 10. 相关文件索引

| 文件 | 说明 |
|------|------|
| `sql/init.sql` | MySQL + Doris 模拟表初始化 |
| `sql/migrate_doris.sql` | Doris 模拟层增量迁移 |
| `services/id-mapping/main.py` | 本地 Flink Job 模拟实现 |
| `tests/test_user_profile_realtime.py` | 用户画像 E2E 测试 |
| `docs/flink/README.md` | Flink Job 模板使用说明 |
| `docs/flink/sql/` | Flink SQL Job 模板 |
| `docs/flink/src/main/java/` | DataStream Job 模板（Maven 标准目录） |

---

文档版本: v1.1 | 多租户用户画像实时链路

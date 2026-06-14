# Flink Job 模板

本目录提供生产环境 Flink Job 模板，对应 `docs/design.md` 中的三个 Job。

## 目录结构

```
docs/flink/
├── README.md
├── pom.xml
├── sql/
│   ├── job-01-ddl-connectors.sql    # Connector / 源表 / 结果表 DDL
│   ├── job-02-profile-aggregation.sql
│   └── job-03-wide-table.sql
└── src/main/java/com/agenticdmp/flink/
    ├── UserEvent.java               # 入站事件模型
    ├── EnrichedEvent.java           # 富化事件模型
    ├── IdMappingFunction.java       # 核心合并逻辑
    └── IdMappingJob.java            # Job-1 入口
```

## Job 分工

| Job | 实现方式 | 原因 |
|-----|---------|------|
| Job-1 ID-Mapping | **DataStream** | 含 Redis 查询、跨渠道 merge 状态机 |
| Job-2 画像聚合 | **Flink SQL** | GROUP BY one_id 聚合，适合 SQL |
| Job-3 宽表打宽 | **Flink SQL** | 列展开 + JOIN，适合 SQL |

## 部署前配置

在 StreamPark 或 `flink-conf.yaml` 中设置：

```properties
# Kafka
kafka.bootstrap.servers=kafka:9092
kafka.source.topic=tenant-1001-events
kafka.sink.topic=enriched-1001-events

# Redis
redis.host=redis
redis.port=6379
redis.ttl.seconds=2592000

# Doris
doris.fe.nodes=fe1:8030
doris.jdbc.url=jdbc:mysql://fe1:9030/tenant_1001
doris.username=root
doris.password=

# MySQL（发号器）
mysql.jdbc.url=jdbc:mysql://mysql:3306/agenticdmp
mysql.username=agenticdmp
mysql.password=agenticdmp123

# 租户
tenant.id=1001
```

## 本地编译 DataStream Job

```bash
cd docs/flink
mvn clean package -DskipTests
# 产出: target/user-profile-flink-jobs-1.0.0.jar
```

提交：

```bash
flink run -c com.agenticdmp.flink.IdMappingJob \
  target/user-profile-flink-jobs-1.0.0.jar \
  --tenant-id 1001 \
  --kafka-source tenant-1001-events \
  --kafka-sink enriched-1001-events
```

## 提交 Flink SQL Job

```bash
# 1. 先执行 DDL 注册 Connector
sql-client.sh -f sql/job-01-ddl-connectors.sql

# 2. 画像聚合
sql-client.sh -f sql/job-02-profile-aggregation.sql

# 3. 宽表打宽
sql-client.sh -f sql/job-03-wide-table.sql
```

## 与本地 Python 模拟对照

| Python (`main.py`) | Flink 模板 |
|--------------------|-----------|
| `IdMappingService.process_event()` | `IdMappingFunction` |
| `upsert_profile()` | `job-02-profile-aggregation.sql` |
| `sync_to_doris()` | `job-03-wide-table.sql` |

本地验证：`bash scripts/run_profile_test.sh`

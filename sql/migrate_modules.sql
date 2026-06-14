-- ════════════════════════════════════════════════════════════════════════
-- migrate_modules.sql — Twilio Segment 对标·全模块 Schema 总集成（增量迁移）
-- ------------------------------------------------------------------------
-- 原则：
--   1) 只做加法，不破坏现有真实功能。所有 CREATE TABLE IF NOT EXISTS。
--   2) 多租户：每张表含 tenant_id，索引/唯一键均带 tenant_id 前缀。
--   3) 字符集统一 utf8mb4 / utf8mb4_unicode_ci，含中文 COMMENT。
--   4) 对既有表（tenants / tag_definitions / user_groups）的新列，
--      通过下方幂等存储过程 _add_col / _add_idx 添加 —— 重复执行安全，
--      不会报 duplicate column / duplicate key（信息架构预检后再 ALTER）。
--   5) 既有表（user_group_members 等）一律不改 PK，新能力走新建关联表。
-- 模块：00-platform 01-connections 02-unify 03-objects 04-accounts
--       05-engage 06-protocols 07-privacy 08-monitor 09-settings
-- ════════════════════════════════════════════════════════════════════════
USE agenticdmp;

-- ── 幂等 DDL 辅助存储过程（添加列 / 添加索引时先检查信息架构）──────────────
DROP PROCEDURE IF EXISTS _add_col;
DROP PROCEDURE IF EXISTS _add_idx;
DELIMITER $$
CREATE PROCEDURE _add_col(IN p_tbl VARCHAR(64), IN p_col VARCHAR(64), IN p_ddl VARCHAR(2048))
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = p_tbl AND COLUMN_NAME = p_col
    ) THEN
        SET @s = CONCAT('ALTER TABLE `', p_tbl, '` ADD COLUMN ', p_ddl);
        PREPARE st FROM @s; EXECUTE st; DEALLOCATE PREPARE st;
    END IF;
END$$
CREATE PROCEDURE _add_idx(IN p_tbl VARCHAR(64), IN p_idx VARCHAR(64), IN p_cols VARCHAR(512))
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = p_tbl AND INDEX_NAME = p_idx
    ) THEN
        SET @s = CONCAT('ALTER TABLE `', p_tbl, '` ADD INDEX `', p_idx, '` (', p_cols, ')');
        PREPARE st FROM @s; EXECUTE st; DEALLOCATE PREPARE st;
    END IF;
END$$
DELIMITER ;

-- ════════════════════════════════════════════════════════════════════════
-- 00 · platform — 租户配置 / 审计 / 租户表生命周期扩展
-- ════════════════════════════════════════════════════════════════════════

-- 既有 tenants 表扩展（生命周期 + 元数据 + 工作区设置）
CALL _add_col('tenants','status',        "status ENUM('active','suspended') NOT NULL DEFAULT 'active' COMMENT '租户状态，停用后网关可拦截'");
CALL _add_col('tenants','scale_tier',    "scale_tier VARCHAR(32) DEFAULT 'dev' COMMENT 'dev/medium/large/xlarge，驱动容量配置'");
CALL _add_col('tenants','contact_email', "contact_email VARCHAR(128) COMMENT '租户联系人'");
CALL _add_col('tenants','description',   "description VARCHAR(512) COMMENT '租户描述'");
CALL _add_col('tenants','max_events_qps',"max_events_qps INT DEFAULT 10000 COMMENT 'QPS 上限（软限）'");
CALL _add_col('tenants','region',        "region VARCHAR(64) COMMENT '区域'");
CALL _add_col('tenants','plan',          "plan ENUM('starter','business','enterprise') DEFAULT 'business' COMMENT '套餐'");
CALL _add_col('tenants','slug',          "slug VARCHAR(128) COMMENT '工作区 URL 标识符'");
CALL _add_col('tenants','updated_at',    "updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'");
CALL _add_idx('tenants','idx_status',    "status");
CALL _add_idx('tenants','idx_slug',      "slug");

-- 每租户独立配置存储（运行时热加载，避免全局 env 污染）
CREATE TABLE IF NOT EXISTS tenant_config (
    tenant_id     BIGINT       NOT NULL,
    config_domain VARCHAR(32)  NOT NULL COMMENT '基础/数据通道/容量/ID-Mapping/存储/隐私/集成/配额',
    config_key    VARCHAR(64)  NOT NULL COMMENT 'kafka_topic/scale_tier/confidence_threshold/olap_backend 等',
    config_value  JSON                  COMMENT '字段值或嵌套配置对象',
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    updated_by    VARCHAR(128)          COMMENT '更新者标识（预留鉴权）',
    PRIMARY KEY (tenant_id, config_domain, config_key),
    INDEX idx_domain (tenant_id, config_domain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='租户级配置存储';

-- 配置变更审计日志
CREATE TABLE IF NOT EXISTS tenant_audit (
    audit_id   BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id  BIGINT      NOT NULL,
    actor      VARCHAR(128) NOT NULL COMMENT '操作者邮箱或 user_id',
    action     VARCHAR(32)  NOT NULL COMMENT 'create/update/suspend/resume',
    target     VARCHAR(64)  NOT NULL COMMENT '变更目标：config_domain 或 status',
    old_value  JSON,
    new_value  JSON,
    reason     VARCHAR(256)          COMMENT '变更原因/备注',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tenant_time (tenant_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='租户配置变更审计';

-- ════════════════════════════════════════════════════════════════════════
-- 01 · connections — 数据源 / 目的地 / Reverse-ETL / 数仓 / 函数 / 管道
-- ════════════════════════════════════════════════════════════════════════

-- 数据源实例
CREATE TABLE IF NOT EXISTS connections_sources (
    tenant_id       BIGINT      NOT NULL,
    source_id       VARCHAR(64) NOT NULL,
    source_name     VARCHAR(128),
    source_type     VARCHAR(64)          COMMENT 'csv/mysql/kafka/api/javascript 等',
    write_key       VARCHAR(128)         COMMENT '租户内唯一写入密钥',
    config          JSON                 COMMENT '连接配置（host/topic/url/headers/template 等）',
    schema_def      JSON                 COMMENT '从样本推断的字段类型',
    status          VARCHAR(32) DEFAULT 'active' COMMENT 'active/paused/error',
    last_event_time DATETIME,
    event_count_24h INT DEFAULT 0,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, source_id),
    UNIQUE KEY uk_write_key (tenant_id, write_key),
    INDEX idx_source_type (tenant_id, source_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='连接·数据源实例';

-- 数据源实时事件流（Debugger 用，抽样/窗口存储）
CREATE TABLE IF NOT EXISTS connections_source_events (
    tenant_id       BIGINT      NOT NULL,
    source_id       VARCHAR(64) NOT NULL,
    event_id        VARCHAR(64) NOT NULL COMMENT 'uuid',
    event_type      VARCHAR(128),
    event_timestamp DATETIME,
    anonymous_id    VARCHAR(128),
    user_id         VARCHAR(128),
    data            JSON                 COMMENT '事件负载',
    status          VARCHAR(32) DEFAULT 'success' COMMENT 'success/error',
    error_msg       VARCHAR(512),
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, source_id, event_id),
    INDEX idx_recent (tenant_id, source_id, created_at),
    INDEX idx_event_type (tenant_id, source_id, event_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='连接·数据源事件流（Debugger）';

-- 目的地实例
CREATE TABLE IF NOT EXISTS connections_destinations (
    tenant_id        BIGINT      NOT NULL,
    destination_id   VARCHAR(64) NOT NULL,
    destination_name VARCHAR(128),
    destination_type VARCHAR(64)          COMMENT 'ads/marketing/bi/webhook',
    config           JSON                 COMMENT 'API key/endpoint/headers/credentials',
    enabled          TINYINT DEFAULT 1,
    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, destination_id),
    INDEX idx_destination_type (tenant_id, destination_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='连接·目的地实例';

-- 目的地字段映射
CREATE TABLE IF NOT EXISTS connections_destination_mappings (
    tenant_id      BIGINT      NOT NULL,
    mapping_id     VARCHAR(64) NOT NULL,
    destination_id VARCHAR(64) NOT NULL,
    source_object  VARCHAR(64)          COMMENT '源对象/segment',
    target_field   VARCHAR(128)         COMMENT '目的地 API 字段',
    source_field   VARCHAR(128)         COMMENT '源字段或常量值',
    transform_logic JSON,
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, mapping_id),
    INDEX idx_destination (tenant_id, destination_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='连接·目的地字段映射';

-- 目的地投递日志
CREATE TABLE IF NOT EXISTS connections_delivery_logs (
    tenant_id        BIGINT      NOT NULL,
    log_id           VARCHAR(64) NOT NULL,
    destination_id   VARCHAR(64),
    batch_id         VARCHAR(64),
    record_count     INT DEFAULT 0,
    success_count    INT DEFAULT 0,
    failed_count     INT DEFAULT 0,
    status           VARCHAR(32) DEFAULT 'pending' COMMENT 'pending/success/partial/failed',
    error_msg        VARCHAR(512),
    attempt_time     DATETIME,
    response_time_ms INT,
    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, log_id),
    INDEX idx_destination (tenant_id, destination_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='连接·目的地投递日志';

-- Reverse-ETL 任务定义
CREATE TABLE IF NOT EXISTS connections_reverse_etl_jobs (
    tenant_id         BIGINT      NOT NULL,
    job_id            VARCHAR(64) NOT NULL,
    job_name          VARCHAR(128),
    source_object     VARCHAR(64)          COMMENT 'object_account/object_order/doris_user_wide',
    destination_id    VARCHAR(64),
    schedule_cron     VARCHAR(64)          COMMENT '如 0 */15 * * * *',
    enabled           TINYINT DEFAULT 1,
    last_run_time     DATETIME,
    next_run_time     DATETIME,
    last_status       VARCHAR(32) DEFAULT 'pending' COMMENT 'pending/success/failed',
    last_error_msg    VARCHAR(512),
    total_synced_rows BIGINT DEFAULT 0     COMMENT '累计同步行数',
    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, job_id),
    INDEX idx_schedule (tenant_id, enabled, next_run_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='连接·Reverse-ETL 任务';

-- Reverse-ETL 执行历史
CREATE TABLE IF NOT EXISTS connections_reverse_etl_runs (
    tenant_id   BIGINT      NOT NULL,
    run_id      VARCHAR(64) NOT NULL,
    job_id      VARCHAR(64),
    start_time  DATETIME,
    end_time    DATETIME,
    duration_ms INT,
    row_count   INT,
    status      VARCHAR(32) DEFAULT 'success' COMMENT 'success/failed/partial',
    error_msg   VARCHAR(512),
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, run_id),
    INDEX idx_job (tenant_id, job_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='连接·Reverse-ETL 执行历史';

-- 数据仓库连接
CREATE TABLE IF NOT EXISTS connections_warehouses (
    tenant_id              BIGINT      NOT NULL,
    warehouse_id           VARCHAR(64) NOT NULL,
    warehouse_name         VARCHAR(128),
    warehouse_type         VARCHAR(32)          COMMENT 'doris/mysql/postgres/hive',
    connection_string      VARCHAR(512)         COMMENT '加密存储',
    username               VARCHAR(256)         COMMENT '加密存储',
    password               VARCHAR(256)         COMMENT '加密存储',
    database_name          VARCHAR(128),
    status                 VARCHAR(32) DEFAULT 'not_connected' COMMENT 'healthy/error/not_connected',
    last_sync_time         DATETIME,
    sync_frequency_seconds INT,
    tables_synced          JSON,
    created_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at             DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, warehouse_id),
    INDEX idx_warehouse_type (tenant_id, warehouse_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='连接·数据仓库';

-- 自定义转换函数
CREATE TABLE IF NOT EXISTS connections_functions (
    tenant_id       BIGINT      NOT NULL,
    function_id     VARCHAR(64) NOT NULL,
    function_name   VARCHAR(128),
    function_type   VARCHAR(32)          COMMENT 'source_function/destination_function',
    language        VARCHAR(32)          COMMENT 'javascript/python',
    code            TEXT                 COMMENT '完整源码',
    runtime_version VARCHAR(32),
    status          VARCHAR(32) DEFAULT 'draft' COMMENT 'draft/deployed',
    entry_point     VARCHAR(128)         COMMENT '调用入口函数名',
    created_by      VARCHAR(128),
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, function_id),
    INDEX idx_function_type (tenant_id, function_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='连接·自定义函数';

-- 函数执行历史
CREATE TABLE IF NOT EXISTS connections_function_runs (
    tenant_id   BIGINT      NOT NULL,
    run_id      VARCHAR(64) NOT NULL,
    function_id VARCHAR(64),
    input       JSON,
    output      JSON,
    status      VARCHAR(32) DEFAULT 'success' COMMENT 'success/error',
    error_msg   VARCHAR(512),
    duration_ms INT,
    memory_mb   INT,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, run_id),
    INDEX idx_function (tenant_id, function_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='连接·函数执行历史';

-- 可视化 ETL 管道拓扑
CREATE TABLE IF NOT EXISTS connections_pipelines (
    tenant_id          BIGINT      NOT NULL,
    pipeline_id        VARCHAR(64) NOT NULL,
    pipeline_name      VARCHAR(128),
    nodes              JSON                 COMMENT '节点数组 {id,type,position,config}',
    edges              JSON                 COMMENT '连线数组 {id,source,target}',
    status             VARCHAR(32) DEFAULT 'draft' COMMENT 'draft/active/paused',
    last_executed_time DATETIME,
    execution_count    INT DEFAULT 0,
    created_by         VARCHAR(128),
    created_at         DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at         DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, pipeline_id),
    INDEX idx_status (tenant_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='连接·ETL 管道拓扑';

-- ════════════════════════════════════════════════════════════════════════
-- 02 · unify — 标签/群组泛对象化、身份解析规则、SQL 特征、预测模型
-- ════════════════════════════════════════════════════════════════════════

-- 既有 tag_definitions / user_groups 扩展为支持任意对象类型
CALL _add_col('tag_definitions','object_type', "object_type VARCHAR(32) NOT NULL DEFAULT '*' COMMENT '适用对象类型，*=全部'");
CALL _add_idx('tag_definitions','idx_object_type', "tenant_id, object_type");
CALL _add_col('user_groups','member_object_type', "member_object_type VARCHAR(32) NOT NULL DEFAULT 'user' COMMENT '成员对象类型'");
CALL _add_idx('user_groups','idx_member_type', "tenant_id, member_object_type");

-- 任意对象标签关联表（不动既有 user 标签链路，新增泛对象关联）
CREATE TABLE IF NOT EXISTS object_tags (
    tenant_id   BIGINT      NOT NULL,
    object_type VARCHAR(32) NOT NULL COMMENT 'user/lead/account/product/store/order',
    object_id   VARCHAR(64) NOT NULL,
    tag_code    VARCHAR(64) NOT NULL,
    source      VARCHAR(32) DEFAULT 'manual' COMMENT 'manual/computed/imported',
    assigned_by VARCHAR(128),
    assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, object_type, object_id, tag_code),
    INDEX idx_object (tenant_id, object_type, object_id),
    INDEX idx_tag (tenant_id, tag_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任意对象标签关联';

-- 泛对象群组成员（不改既有 user_group_members 的 PK，新增泛对象成员表）
CREATE TABLE IF NOT EXISTS object_group_members (
    tenant_id   BIGINT      NOT NULL,
    group_id    BIGINT      NOT NULL,
    object_type VARCHAR(32) NOT NULL COMMENT 'user/lead/account/...',
    object_id   VARCHAR(64) NOT NULL,
    source      VARCHAR(32) DEFAULT 'manual' COMMENT 'manual/dynamic/imported',
    added_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, group_id, object_type, object_id),
    INDEX idx_object (tenant_id, object_type, object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='泛对象群组成员';

-- 身份解析规则（merge 策略与优先级）
CREATE TABLE IF NOT EXISTS identity_resolution_rules (
    tenant_id       BIGINT      NOT NULL,
    rule_id         VARCHAR(64) NOT NULL,
    identifier_type VARCHAR(32) NOT NULL COMMENT 'wechat_openid/phone 等',
    priority        INT DEFAULT 50       COMMENT '1-100',
    max_per_profile INT                  COMMENT '合并上限',
    is_unique       TINYINT DEFAULT 0,
    is_primary      TINYINT DEFAULT 0    COMMENT '是否主标识',
    merge_strategy  VARCHAR(32)          COMMENT 'take_min/take_max/latest',
    description     VARCHAR(256),
    enabled         TINYINT DEFAULT 1,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, rule_id),
    UNIQUE KEY uk_identifier_type (tenant_id, identifier_type),
    INDEX idx_priority (tenant_id, priority)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='身份解析规则';

-- SQL 特征定义（定时/手动计算落库）
CREATE TABLE IF NOT EXISTS sql_trait_definitions (
    tenant_id      BIGINT      NOT NULL,
    trait_id       VARCHAR(64) NOT NULL,
    trait_code     VARCHAR(64) NOT NULL,
    trait_name     VARCHAR(128),
    sql_query      TEXT,
    warehouse_type VARCHAR(32)          COMMENT 'doris/mysql/hive',
    warehouse_id   VARCHAR(64),
    schedule_type  VARCHAR(32) DEFAULT 'manual' COMMENT 'manual/hourly/daily',
    schedule_cron  VARCHAR(64),
    result_table   VARCHAR(128),
    last_run_time  DATETIME,
    last_row_count INT,
    enabled        TINYINT DEFAULT 1,
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, trait_id),
    UNIQUE KEY uk_trait_code (tenant_id, trait_code),
    INDEX idx_schedule (tenant_id, schedule_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='SQL 特征定义';

-- SQL 特征计算结果
CREATE TABLE IF NOT EXISTS sql_trait_results (
    tenant_id   BIGINT      NOT NULL,
    trait_id    VARCHAR(64) NOT NULL,
    object_type VARCHAR(32) NOT NULL,
    object_id   VARCHAR(64) NOT NULL,
    trait_value VARCHAR(512),
    computed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    version     INT DEFAULT 1,
    PRIMARY KEY (tenant_id, trait_id, object_type, object_id),
    INDEX idx_trait (tenant_id, trait_id),
    INDEX idx_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='SQL 特征结果';

-- 预测模型配置
CREATE TABLE IF NOT EXISTS prediction_models (
    tenant_id          BIGINT      NOT NULL,
    model_id           VARCHAR(64) NOT NULL,
    model_name         VARCHAR(128),
    model_type         VARCHAR(32)          COMMENT 'purchase/churn/ltv',
    target_event       VARCHAR(128),
    features           JSON,
    training_data_days INT                  COMMENT '回溯天数',
    inference_horizon  VARCHAR(32)          COMMENT '预测周期',
    quality_score      DECIMAL(5,2)         COMMENT '模型质量',
    last_training_at   DATETIME,
    last_inference_at  DATETIME,
    enabled            TINYINT DEFAULT 1,
    created_at         DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at         DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, model_id),
    UNIQUE KEY uk_model_name (tenant_id, model_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='预测模型';

-- ════════════════════════════════════════════════════════════════════════
-- 03 · objects — 对象/字段/关系 注册表（数据驱动 OBJECT_REGISTRY/RELATION_MATRIX）
-- ════════════════════════════════════════════════════════════════════════

-- 对象注册（内置 + 用户自建）
CREATE TABLE IF NOT EXISTS object_definitions (
    tenant_id   BIGINT      NOT NULL,
    object_key  VARCHAR(64) NOT NULL COMMENT "如 'coupon'",
    label       VARCHAR(128)         COMMENT '显示名',
    table_name  VARCHAR(64)          COMMENT '物理表名',
    pk          VARCHAR(64)          COMMENT '主键字段名',
    icon        VARCHAR(32)          COMMENT 'lucide 图标名',
    is_builtin  TINYINT DEFAULT 0    COMMENT '1=内置(禁删/禁改PK)，0=自建',
    sort_order  INT DEFAULT 0,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, object_key),
    INDEX idx_builtin (tenant_id, is_builtin)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='对象注册表';

-- 对象字段定义
CREATE TABLE IF NOT EXISTS object_fields (
    tenant_id     BIGINT      NOT NULL,
    object_key    VARCHAR(64) NOT NULL,
    field_code    VARCHAR(64) NOT NULL COMMENT "如 'sku'",
    field_type    ENUM('int','float','str','json','json_array','datetime') NOT NULL DEFAULT 'str',
    is_required   TINYINT DEFAULT 0,
    default_value VARCHAR(256),
    field_label   VARCHAR(128),
    is_active     TINYINT DEFAULT 1    COMMENT '软删除以保证向后兼容',
    sort_order    INT DEFAULT 0,
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, object_key, field_code),
    INDEX idx_active (tenant_id, object_key, is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='对象字段定义';

-- 关系矩阵（声明式）
CREATE TABLE IF NOT EXISTS relation_definitions (
    tenant_id      BIGINT      NOT NULL,
    src_type       VARCHAR(64) NOT NULL,
    rel_type       VARCHAR(64) NOT NULL COMMENT "如 'contains'/'belongs_to'",
    dst_type       VARCHAR(64) NOT NULL,
    relation_label VARCHAR(256)         COMMENT "如 '订单包含商品'",
    is_builtin     TINYINT DEFAULT 1    COMMENT '内置禁删',
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, src_type, rel_type, dst_type),
    INDEX idx_src (tenant_id, src_type),
    INDEX idx_dst (tenant_id, dst_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='对象关系定义';

-- 关系边属性 schema
CREATE TABLE IF NOT EXISTS relation_properties (
    tenant_id  BIGINT      NOT NULL,
    src_type   VARCHAR(64) NOT NULL,
    rel_type   VARCHAR(64) NOT NULL,
    dst_type   VARCHAR(64) NOT NULL,
    prop_key   VARCHAR(64) NOT NULL COMMENT "如 'quantity'",
    prop_type  ENUM('int','float','str','json','datetime') NOT NULL DEFAULT 'str',
    prop_label VARCHAR(256)         COMMENT "如 '购买数量'",
    sort_order INT DEFAULT 0,
    PRIMARY KEY (tenant_id, src_type, rel_type, dst_type, prop_key),
    INDEX idx_relation (tenant_id, src_type, rel_type, dst_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='关系边属性 schema';

-- ════════════════════════════════════════════════════════════════════════
-- 04 · accounts — 账户聚合 / 层级 / 合并日志
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS account_aggregates (
    tenant_id         BIGINT      NOT NULL,
    account_id        VARCHAR(64) NOT NULL,
    user_count        INT DEFAULT 0,
    active_user_count INT DEFAULT 0,
    total_gmv         DECIMAL(12,2) DEFAULT 0,
    purchase_count    INT DEFAULT 0,
    product_count     INT DEFAULT 0,
    channel_count     INT DEFAULT 0,
    tags              JSON,
    properties        JSON,
    last_update_time  DATETIME,
    metric_date       DATE,
    PRIMARY KEY (tenant_id, account_id),
    INDEX idx_date (tenant_id, metric_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='账户级聚合指标';

CREATE TABLE IF NOT EXISTS account_hierarchy (
    tenant_id         BIGINT      NOT NULL,
    account_id        VARCHAR(64) NOT NULL,
    parent_account_id VARCHAR(64),
    level             INT DEFAULT 1,
    path              VARCHAR(512) COMMENT '层级路径，如 A3001/A3002/A3003',
    relationship_type VARCHAR(32)  COMMENT 'group/subsidiary/affiliate',
    properties        JSON,
    create_time       DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time       DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, account_id),
    INDEX idx_parent (tenant_id, parent_account_id),
    INDEX idx_level (tenant_id, level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='账户父子层级';

CREATE TABLE IF NOT EXISTS account_merge_log (
    tenant_id         BIGINT      NOT NULL,
    master_account_id VARCHAR(64) NOT NULL COMMENT '合并后目标账户',
    merged_account_id VARCHAR(64) NOT NULL COMMENT '被合并源账户',
    action            VARCHAR(32)          COMMENT 'merge/dedup/unmerge',
    merged_fields     JSON                 COMMENT '被合并字段',
    user_count        INT,
    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_by        VARCHAR(64),
    PRIMARY KEY (tenant_id, master_account_id, merged_account_id),
    INDEX idx_created (tenant_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='账户合并日志';

-- ════════════════════════════════════════════════════════════════════════
-- 05 · engage — 受众规模快照 / 受众→目的地 / 旅程 / 群发
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS audience_size_snapshot (
    snapshot_id  BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id    BIGINT NOT NULL,
    segment_id   BIGINT NOT NULL,
    segment_code VARCHAR(64),
    size         INT DEFAULT 0,
    estimate_ms  INT,
    created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_segment_time (tenant_id, segment_id, created_at),
    INDEX idx_segment (tenant_id, segment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='受众规模趋势快照';

CREATE TABLE IF NOT EXISTS segment_destinations (
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id        BIGINT NOT NULL,
    segment_id       BIGINT NOT NULL,
    destination_id   VARCHAR(64) NOT NULL,
    destination_name VARCHAR(128),
    destination_type VARCHAR(64),
    status           VARCHAR(32) DEFAULT 'active',
    mapped_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_sync        DATETIME,
    sync_count       INT DEFAULT 0,
    UNIQUE KEY uk_seg_dest (tenant_id, segment_id, destination_id),
    INDEX idx_segment (tenant_id, segment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='受众与目的地关联';

CREATE TABLE IF NOT EXISTS journeys (
    journey_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id         BIGINT NOT NULL,
    journey_code      VARCHAR(64) NOT NULL,
    journey_name      VARCHAR(128),
    description       VARCHAR(512),
    trigger_type      VARCHAR(32)  COMMENT 'segment_entry/event/schedule',
    trigger_condition JSON,
    base_segment_id   BIGINT,
    visual_config     JSON,
    status            VARCHAR(32) DEFAULT 'draft' COMMENT 'draft/active/paused/archived',
    created_by        VARCHAR(128),
    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_journey_code (tenant_id, journey_code),
    INDEX idx_status (tenant_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='旅程定义';

CREATE TABLE IF NOT EXISTS journey_steps (
    step_id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    journey_id          BIGINT NOT NULL,
    tenant_id           BIGINT NOT NULL,
    step_order          INT DEFAULT 0,
    step_type           VARCHAR(32)  COMMENT 'action/wait/split/exit',
    step_name           VARCHAR(128),
    action_type         VARCHAR(32),
    destination_id      VARCHAR(64),
    wait_duration_hours INT,
    condition_logic     VARCHAR(32)  COMMENT 'and/or',
    conditions          JSON,
    next_steps          JSON,
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_journey (journey_id, tenant_id),
    INDEX idx_order (tenant_id, journey_id, step_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='旅程步骤';

CREATE TABLE IF NOT EXISTS journey_state (
    state_id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    journey_id             BIGINT NOT NULL,
    tenant_id              BIGINT NOT NULL,
    one_id                 BIGINT NOT NULL,
    step_id                BIGINT,
    entered_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
    completed_at           DATETIME,
    status                 VARCHAR(32) DEFAULT 'active' COMMENT 'active/completed/exited',
    split_condition_result VARCHAR(64),
    UNIQUE KEY uk_journey_one (journey_id, one_id),
    INDEX idx_journey (tenant_id, journey_id),
    INDEX idx_one (one_id, journey_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户旅程运行状态';

CREATE TABLE IF NOT EXISTS broadcasts (
    broadcast_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id        BIGINT NOT NULL,
    broadcast_code   VARCHAR(64) NOT NULL,
    broadcast_name   VARCHAR(128),
    segment_id       BIGINT,
    destination_id   VARCHAR(64),
    channel_type     VARCHAR(32)  COMMENT 'email/sms/push/wechat',
    subject          VARCHAR(256),
    content_template TEXT,
    estimated_size   INT DEFAULT 0,
    sent_count       INT DEFAULT 0,
    bounce_count     INT DEFAULT 0,
    open_count       INT DEFAULT 0,
    status           VARCHAR(32) DEFAULT 'draft' COMMENT 'draft/scheduled/sending/sent/failed',
    scheduled_at     DATETIME,
    sent_at          DATETIME,
    created_by       VARCHAR(128),
    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_broadcast_code (tenant_id, broadcast_code),
    INDEX idx_status (tenant_id, status),
    INDEX idx_destination (tenant_id, destination_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='群发任务';

CREATE TABLE IF NOT EXISTS broadcast_sends (
    send_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
    broadcast_id   BIGINT NOT NULL,
    tenant_id      BIGINT NOT NULL,
    one_id         BIGINT NOT NULL,
    destination_id VARCHAR(64),
    channel_type   VARCHAR(32),
    sent_at        DATETIME,
    delivered_at   DATETIME,
    bounced_at     DATETIME,
    opened_at      DATETIME,
    clicked_at     DATETIME,
    status         VARCHAR(32) DEFAULT 'pending' COMMENT 'pending/sent/delivered/bounced/opened/clicked',
    error_message  VARCHAR(512),
    UNIQUE KEY uk_broadcast_one (broadcast_id, one_id),
    INDEX idx_broadcast (broadcast_id, tenant_id),
    INDEX idx_destination (tenant_id, destination_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='单条群发记录与回执';

-- ════════════════════════════════════════════════════════════════════════
-- 06 · protocols — 埋点计划 / 事件 schema / 违规 / 转换
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS tracking_plans (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id   BIGINT NOT NULL,
    name        VARCHAR(128) NOT NULL,
    description VARCHAR(512),
    sources     JSON COMMENT '数据源列表，如 ["app","web","小程序"]',
    enabled     TINYINT DEFAULT 1,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_tenant_name (tenant_id, name),
    INDEX idx_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='埋点计划';

CREATE TABLE IF NOT EXISTS tracking_plan_events (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id       BIGINT NOT NULL,
    plan_id         BIGINT NOT NULL,
    event           VARCHAR(128) NOT NULL COMMENT '事件名，如 Order Completed',
    type            ENUM('track','identify') NOT NULL DEFAULT 'track' COMMENT '事件类型',
    properties_json JSON COMMENT '{"property_name":"type",...}',
    required        JSON COMMENT '["必填属性1","必填属性2"]',
    status          ENUM('draft','approved') DEFAULT 'draft' COMMENT '审批状态',
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_plan_event (plan_id, event),
    INDEX idx_plan (tenant_id, plan_id),
    INDEX idx_event (tenant_id, event)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='埋点计划事件 schema';

CREATE TABLE IF NOT EXISTS violations (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id  BIGINT NOT NULL,
    event      VARCHAR(128) NOT NULL COMMENT '被违规的事件名',
    issue      VARCHAR(256) NOT NULL COMMENT '问题描述',
    count      INT DEFAULT 1 COMMENT '累计出现次数',
    source     VARCHAR(64) COMMENT '数据来源',
    severity   ENUM('high','low') DEFAULT 'low' COMMENT '严重级别',
    first_seen DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '首次出现',
    last_seen  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最近出现',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_tenant_event_issue (tenant_id, event, issue),
    INDEX idx_tenant (tenant_id),
    INDEX idx_severity (tenant_id, severity),
    INDEX idx_source (tenant_id, source),
    INDEX idx_last_seen (last_seen)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='数据质量违规聚合';

CREATE TABLE IF NOT EXISTS transformations (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id   BIGINT NOT NULL,
    name        VARCHAR(128) NOT NULL COMMENT '转换规则名',
    scope       VARCHAR(128) COMMENT '作用范围：某事件或 all_events',
    type        ENUM('rename','delete','mapping') NOT NULL DEFAULT 'rename' COMMENT '转换类型',
    config      JSON COMMENT '规则配置',
    enabled     TINYINT DEFAULT 1,
    description VARCHAR(512),
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_tenant (tenant_id),
    INDEX idx_scope (tenant_id, scope),
    INDEX idx_enabled (tenant_id, enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='事件转换规则';

-- ════════════════════════════════════════════════════════════════════════
-- 07 · privacy — PII / 同意 / 删除工单 / 抑制名单 / 审计
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS pii_rules (
    rule_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id      BIGINT NOT NULL,
    field_name     VARCHAR(128) NOT NULL,
    category       VARCHAR(64)  COMMENT 'PII 分类',
    action         VARCHAR(32)  COMMENT 'mask/hash/drop/encrypt',
    scope          VARCHAR(64),
    source         VARCHAR(64),
    target_objects JSON,
    created_by     VARCHAR(128),
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_active      TINYINT DEFAULT 1,
    UNIQUE KEY uk_tenant_field (tenant_id, field_name),
    INDEX idx_tenant (tenant_id),
    INDEX idx_active (tenant_id, is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='PII 字段管控规则';

CREATE TABLE IF NOT EXISTS consent_categories (
    category_id   BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id     BIGINT NOT NULL,
    category_name VARCHAR(128) NOT NULL,
    description   VARCHAR(512),
    is_required   TINYINT DEFAULT 0,
    vendor_list   JSON COMMENT '厂商/目的地映射',
    created_by    VARCHAR(128),
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_tenant_cat (tenant_id, category_name),
    INDEX idx_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='同意分类定义';

CREATE TABLE IF NOT EXISTS consent_records (
    record_id    BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id    BIGINT NOT NULL,
    one_id       BIGINT,
    identifier   VARCHAR(256),
    category_id  BIGINT NOT NULL,
    granted      TINYINT DEFAULT 0,
    withdrawn_at DATETIME,
    created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_tenant_one_category (tenant_id, one_id, category_id),
    INDEX idx_tenant_one (tenant_id, one_id),
    INDEX idx_category (tenant_id, category_id),
    INDEX idx_identifier (tenant_id, identifier(64))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='主体级同意记录';

CREATE TABLE IF NOT EXISTS deletion_requests (
    request_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id       BIGINT NOT NULL,
    identifier      VARCHAR(256),
    one_id          BIGINT,
    request_type    VARCHAR(32) COMMENT 'delete/suppress',
    reason          VARCHAR(512),
    status          VARCHAR(32) DEFAULT 'pending' COMMENT 'pending/processing/completed/failed',
    created_by      VARCHAR(128),
    affected_tables JSON,
    affected_count  INT,
    executed_at     DATETIME,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_tenant (tenant_id),
    INDEX idx_status (tenant_id, status),
    INDEX idx_one_id (tenant_id, one_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='GDPR 删除/抑制工单';

CREATE TABLE IF NOT EXISTS suppression_list (
    suppression_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id           BIGINT NOT NULL,
    identifier          VARCHAR(256),
    one_id              BIGINT,
    suppression_type    VARCHAR(32) COMMENT 'collect/forward/both',
    reason              VARCHAR(512),
    deletion_request_id BIGINT,
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    expires_at          DATETIME,
    UNIQUE KEY uk_tenant_identifier (tenant_id, identifier(64)),
    INDEX idx_tenant (tenant_id),
    INDEX idx_one_id (tenant_id, one_id),
    INDEX idx_expires (tenant_id, expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='抑制名单';

CREATE TABLE IF NOT EXISTS privacy_audit_log (
    audit_id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id           BIGINT NOT NULL,
    operation_type      VARCHAR(32) COMMENT 'delete/suppress/consent_change',
    deletion_request_id BIGINT,
    operator            VARCHAR(128),
    one_id              BIGINT,
    scope               VARCHAR(64),
    affected_records    INT,
    detail              JSON,
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tenant (tenant_id),
    INDEX idx_request (tenant_id, deletion_request_id),
    INDEX idx_operation (tenant_id, operation_type, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='隐私操作审计';

-- ════════════════════════════════════════════════════════════════════════
-- 08 · monitor — 指标聚合 / 告警规则 / 告警事件 / 事件投递日志
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS monitor_metrics (
    tenant_id      BIGINT   NOT NULL,
    bucket_ts      DATETIME NOT NULL COMMENT '分钟/小时桶时间戳',
    source         VARCHAR(128) NOT NULL DEFAULT '' COMMENT '数据源名称',
    events_total   INT DEFAULT 0 COMMENT '桶内事件总数',
    success_count  INT DEFAULT 0,
    failed_count   INT DEFAULT 0,
    latency_ms_p50 INT,
    latency_ms_p95 INT,
    latency_ms_p99 INT,
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, bucket_ts, source),
    INDEX idx_tenant_time (tenant_id, bucket_ts),
    INDEX idx_tenant_source (tenant_id, source, bucket_ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='监控指标聚合';

CREATE TABLE IF NOT EXISTS monitor_alert_rule (
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id      BIGINT NOT NULL,
    name           VARCHAR(256) NOT NULL COMMENT '规则名称',
    metric         VARCHAR(64) NOT NULL COMMENT 'success_rate/event_count/error_rate/latency_p95',
    operator       VARCHAR(32) NOT NULL COMMENT 'lt/gt/eq/gte/lte',
    threshold      DECIMAL(10,2) NOT NULL COMMENT '阈值',
    window_minutes INT DEFAULT 5 COMMENT '评估窗口（分钟）',
    scope          VARCHAR(64) COMMENT 'all_sources/specific_source/specific_destination',
    scope_value    VARCHAR(256),
    channel        VARCHAR(128) NOT NULL COMMENT 'email/feishu/webhook',
    channel_config JSON,
    severity       VARCHAR(32) DEFAULT 'medium' COMMENT 'high/medium/low',
    enabled        TINYINT DEFAULT 1,
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_tenant (tenant_id),
    INDEX idx_tenant_enabled (tenant_id, enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='告警规则';

CREATE TABLE IF NOT EXISTS monitor_alert_event (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id       BIGINT NOT NULL,
    rule_id         BIGINT NOT NULL COMMENT '关联 monitor_alert_rule.id',
    fired_at        DATETIME NOT NULL COMMENT '触发时刻',
    metric_value    DECIMAL(10,2),
    status          VARCHAR(32) DEFAULT 'triggered' COMMENT 'triggered/acknowledged/resolved',
    acknowledged_at DATETIME,
    acknowledged_by VARCHAR(128),
    resolved_at     DATETIME,
    detail          JSON,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_rule_fired (rule_id, fired_at),
    INDEX idx_tenant_fired (tenant_id, fired_at),
    CONSTRAINT fk_alert_rule FOREIGN KEY (rule_id) REFERENCES monitor_alert_rule(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='告警触发记录';

CREATE TABLE IF NOT EXISTS event_delivery_log (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id     BIGINT NOT NULL,
    ts            DATETIME NOT NULL COMMENT '事件处理时刻',
    source        VARCHAR(128) NOT NULL COMMENT '数据源名称',
    event_name    VARCHAR(256) COMMENT '事件类型名',
    destination   VARCHAR(256) COMMENT '目的地名称',
    status        VARCHAR(32) DEFAULT 'success' COMMENT 'success/failed/retry/skipped',
    http_code     INT,
    latency_ms    INT,
    error_message VARCHAR(512),
    event_id      VARCHAR(256) COMMENT '原始事件 ID',
    detail        JSON,
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tenant_ts (tenant_id, ts),
    INDEX idx_source_dest (tenant_id, source, destination, ts),
    INDEX idx_status (tenant_id, status, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='逐事件投递日志';

-- ════════════════════════════════════════════════════════════════════════
-- 09 · settings — 成员 / 角色 / 团队 / API 令牌 / 审计 / 邀请
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS users (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id  BIGINT NOT NULL,
    email      VARCHAR(255) NOT NULL,
    name       VARCHAR(128),
    status     ENUM('active','inactive','pending') DEFAULT 'active',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_tenant_email (tenant_id, email),
    INDEX idx_tenant (tenant_id),
    INDEX idx_status (tenant_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='工作区成员';

CREATE TABLE IF NOT EXISTS roles (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id    BIGINT NOT NULL,
    name         VARCHAR(128) NOT NULL,
    scope        JSON NOT NULL COMMENT 'RBAC 权限范围',
    member_count INT DEFAULT 0,
    created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_tenant_name (tenant_id, name),
    INDEX idx_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='角色定义';

CREATE TABLE IF NOT EXISTS user_roles (
    user_id     BIGINT NOT NULL,
    role_id     BIGINT NOT NULL,
    assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, role_id),
    INDEX idx_role (role_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户-角色关联';

CREATE TABLE IF NOT EXISTS teams (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id   BIGINT NOT NULL,
    name        VARCHAR(128) NOT NULL,
    description VARCHAR(512),
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_tenant_name (tenant_id, name),
    INDEX idx_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='团队';

CREATE TABLE IF NOT EXISTS team_members (
    team_id   BIGINT NOT NULL,
    user_id   BIGINT NOT NULL,
    joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (team_id, user_id),
    INDEX idx_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='团队-成员关联';

CREATE TABLE IF NOT EXISTS api_tokens (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id  BIGINT NOT NULL,
    label      VARCHAR(128) NOT NULL,
    prefix     VARCHAR(16) NOT NULL COMMENT '前8位，供识别',
    hash       VARCHAR(255) NOT NULL COMMENT 'SHA-256 哈希，不存明文',
    scopes     JSON NOT NULL COMMENT '权限范围数组',
    created_by BIGINT NOT NULL COMMENT '创建者 user_id',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_used  DATETIME COMMENT '最后使用时间',
    revoked_at DATETIME COMMENT '吊销时间',
    UNIQUE KEY uk_tenant_prefix (tenant_id, prefix),
    INDEX idx_tenant_active (tenant_id, revoked_at),
    INDEX idx_last_used (tenant_id, last_used)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='服务端 API 令牌';

CREATE TABLE IF NOT EXISTS api_token_usage (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    token_id     BIGINT NOT NULL,
    tenant_id    BIGINT NOT NULL,
    requests_24h INT DEFAULT 0,
    requests_30d INT DEFAULT 0,
    last_ip      VARCHAR(45),
    last_ua      VARCHAR(512),
    updated_at   DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_token (token_id),
    INDEX idx_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='API 令牌使用统计';

CREATE TABLE IF NOT EXISTS audit_log (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id  BIGINT NOT NULL,
    actor      VARCHAR(255) NOT NULL COMMENT '操作者 email 或 system',
    action     VARCHAR(64) NOT NULL COMMENT 'invite_member/issue_token/revoke_token/update_workspace 等',
    target     VARCHAR(256) NOT NULL COMMENT '目标对象',
    module     VARCHAR(64) COMMENT '模块名，如 settings/engage/privacy',
    details    JSON,
    ip_addr    VARCHAR(45),
    user_agent VARCHAR(512),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tenant_time (tenant_id, created_at),
    INDEX idx_actor (tenant_id, actor, created_at),
    INDEX idx_action (tenant_id, action, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='工作区操作审计';

CREATE TABLE IF NOT EXISTS invitations (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id   BIGINT NOT NULL,
    email       VARCHAR(255) NOT NULL,
    role_id     BIGINT NOT NULL COMMENT '邀请时指定角色',
    token       VARCHAR(255) NOT NULL COMMENT '邀请令牌，唯一',
    status      ENUM('pending','accepted','declined','expired') DEFAULT 'pending',
    invited_by  BIGINT NOT NULL COMMENT '邀请人 user_id',
    expires_at  DATETIME NOT NULL,
    accepted_at DATETIME,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_token (token),
    INDEX idx_tenant_status (tenant_id, status, expires_at),
    INDEX idx_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='成员邀请';

-- ── 清理辅助存储过程 ──────────────────────────────────────────────────────
DROP PROCEDURE IF EXISTS _add_col;
DROP PROCEDURE IF EXISTS _add_idx;

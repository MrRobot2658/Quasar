-- 增量迁移：Doris 模拟层（已有环境执行一次即可）
USE agenticdmp;

CREATE TABLE IF NOT EXISTS doris_id_mapping (
    tenant_id       BIGINT NOT NULL,
    channel_type    VARCHAR(32) NOT NULL,
    channel_id      VARCHAR(256) NOT NULL,
    one_id          BIGINT NOT NULL,
    source          VARCHAR(32) DEFAULT 'realtime',
    update_time     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, channel_type, channel_id),
    INDEX idx_one_id (tenant_id, one_id)
) ENGINE=InnoDB COMMENT='Doris id_mapping 模拟表';

CREATE TABLE IF NOT EXISTS doris_user_wide (
    tenant_id           BIGINT NOT NULL,
    one_id              BIGINT NOT NULL,
    wechat_openid       VARCHAR(256),
    wechat_unionid      VARCHAR(256),
    wework_extid        VARCHAR(256),
    form_id             VARCHAR(256) COMMENT '表单留资ID',
    phone               VARCHAR(256),
    email               VARCHAR(256),
    device              VARCHAR(256),
    channel_count       INT DEFAULT 0,
    tags                JSON,
    properties          JSON,
    last_event_time     DATETIME,
    update_time         DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, one_id)
) ENGINE=InnoDB COMMENT='Doris 用户宽表（实时打宽）';

-- 兼容旧环境：补充 form_id 列
SET @col_exists := (
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = 'agenticdmp' AND TABLE_NAME = 'doris_user_wide' AND COLUMN_NAME = 'form_id'
);
SET @ddl := IF(
    @col_exists = 0,
    'ALTER TABLE doris_user_wide ADD COLUMN form_id VARCHAR(256) COMMENT ''表单留资ID'' AFTER wework_extid',
    'SELECT 1'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

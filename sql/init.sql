-- 多租户实时 ID-Mapping 开发环境初始化
-- 业务数据库: MySQL

CREATE DATABASE IF NOT EXISTS agenticdmp DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE agenticdmp;

-- 租户配置
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id       BIGINT PRIMARY KEY,
    tenant_name     VARCHAR(128) NOT NULL,
    tier            ENUM('premium', 'standard') NOT NULL DEFAULT 'standard',
    kafka_topic     VARCHAR(128) NOT NULL,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- ID 映射冷层（对应文档 Doris id_mapping 表）
CREATE TABLE IF NOT EXISTS id_mapping (
    tenant_id       BIGINT NOT NULL,
    channel_type    VARCHAR(32) NOT NULL COMMENT 'wechat_openid/wechat_unionid/wework_extid/form_id/phone/email/device',
    channel_id      VARCHAR(256) NOT NULL,
    one_id          BIGINT NOT NULL,
    confidence      DOUBLE DEFAULT 1.0,
    source          VARCHAR(32) DEFAULT 'realtime' COMMENT 'login/device/ip/algorithm/realtime',
    create_time     DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, channel_type, channel_id),
    INDEX idx_one_id (tenant_id, one_id),
    INDEX idx_channel (channel_type, channel_id)
) ENGINE=InnoDB COMMENT='渠道ID → OneID 映射';

-- OneID 序列表（租户内自增）
CREATE TABLE IF NOT EXISTS one_id_sequence (
    tenant_id       BIGINT PRIMARY KEY,
    next_id         BIGINT NOT NULL DEFAULT 100000
) ENGINE=InnoDB;

-- 用户画像（对应文档 user_profile）
CREATE TABLE IF NOT EXISTS user_profile (
    tenant_id       BIGINT NOT NULL,
    user_id         BIGINT NOT NULL COMMENT 'OneID',
    channel_type    VARCHAR(32),
    channel_id      VARCHAR(128),
    tags            JSON COMMENT '用户标签',
    properties      JSON COMMENT '扩展属性',
    update_time     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, user_id),
    INDEX idx_update_time (update_time)
) ENGINE=InnoDB COMMENT='用户画像';

-- 合并日志（开发调试用）
CREATE TABLE IF NOT EXISTS merge_log (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id       BIGINT NOT NULL,
    event_id        VARCHAR(64),
    action          VARCHAR(32) NOT NULL COMMENT 'create/merge/link',
    one_id          BIGINT NOT NULL,
    channel_type    VARCHAR(32),
    channel_id      VARCHAR(256),
    linked_one_id   BIGINT COMMENT '合并目标 OneID',
    detail          JSON,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tenant_time (tenant_id, created_at)
) ENGINE=InnoDB COMMENT='ID 合并操作日志';

-- 初始化租户（模拟大租户 + 小租户）
INSERT INTO tenants (tenant_id, tenant_name, tier, kafka_topic) VALUES
    (1001, '品牌A（大租户）', 'premium', 'tenant-1001-events'),
    (1002, '品牌B（小租户）', 'standard', 'tenant-1002-events'),
    (1003, '品牌C（共享Topic）', 'standard', 'shared-tenant-events');

INSERT INTO one_id_sequence (tenant_id, next_id) VALUES
    (1001, 100001),
    (1002, 200001),
    (1003, 300001);

-- 预置部分离线 ID 映射（模拟历史数据）
INSERT INTO id_mapping (tenant_id, channel_type, channel_id, one_id, confidence, source) VALUES
    (1001, 'wechat_unionid', 'union_abc123', 100001, 1.0, 'offline'),
    (1001, 'wechat_openid', 'oXxx_offline_001', 100001, 0.95, 'offline'),
    (1001, 'phone', '13800138001', 100001, 1.0, 'login');

INSERT INTO user_profile (tenant_id, user_id, channel_type, channel_id, tags, properties) VALUES
    (1001, 100001, 'wechat_openid', 'oXxx_offline_001',
     JSON_ARRAY('high_value', 'wechat_user'),
     JSON_OBJECT('total_orders', 5, 'total_amount', 12800, 'last_login', '2026-06-01 10:00:00'));

-- Doris 模拟层：冷层映射表（Flink → Doris UNIQUE KEY Upsert）
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

-- Doris 宽表：实时打宽（多渠道 ID + 画像聚合，供联合查询）
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

-- 预置离线用户的 Doris 宽表
INSERT INTO doris_user_wide (tenant_id, one_id, wechat_openid, wechat_unionid, phone, channel_count, tags, properties)
VALUES (1001, 100001, 'oXxx_offline_001', 'union_abc123', '13800138001', 3,
        JSON_ARRAY('high_value', 'wechat_user'),
        JSON_OBJECT('total_orders', 5, 'total_amount', 12800, 'last_login', '2026-06-01 10:00:00'));

-- 用户分组（人群包）
CREATE TABLE IF NOT EXISTS user_groups (
    tenant_id       BIGINT NOT NULL,
    group_id        BIGINT NOT NULL AUTO_INCREMENT,
    group_code      VARCHAR(64) NOT NULL,
    group_name      VARCHAR(128) NOT NULL,
    description     VARCHAR(512),
    group_type      ENUM('static', 'dynamic') NOT NULL DEFAULT 'static',
    filter_rule     JSON,
    member_count    INT NOT NULL DEFAULT 0,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (group_id),
    UNIQUE KEY uk_tenant_code (tenant_id, group_code),
    INDEX idx_tenant (tenant_id)
) ENGINE=InnoDB AUTO_INCREMENT=1001;

CREATE TABLE IF NOT EXISTS user_group_members (
    tenant_id       BIGINT NOT NULL,
    group_id        BIGINT NOT NULL,
    one_id          BIGINT NOT NULL,
    added_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    source          VARCHAR(32) DEFAULT 'manual',
    PRIMARY KEY (tenant_id, group_id, one_id),
    INDEX idx_one_id (tenant_id, one_id)
) ENGINE=InnoDB;

INSERT INTO user_groups (tenant_id, group_id, group_code, group_name, description, group_type, member_count) VALUES
    (1001, 1001, 'vip_high_value', 'VIP高价值用户', '消费金额高、活跃度高的核心用户', 'static', 1),
    (1001, 1002, 'wechat_users', '微信小程序用户', '来自微信渠道的注册用户', 'static', 1),
    (1001, 1003, 'form_leads', '表单留资用户', '通过表单渠道留资的潜在客户', 'dynamic', 0);

INSERT INTO user_group_members (tenant_id, group_id, one_id, source) VALUES
    (1001, 1001, 100001, 'offline'),
    (1001, 1002, 100001, 'offline');

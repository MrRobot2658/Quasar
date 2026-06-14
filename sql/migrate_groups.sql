USE agenticdmp;

CREATE TABLE IF NOT EXISTS user_groups (
    tenant_id       BIGINT NOT NULL,
    group_id        BIGINT NOT NULL AUTO_INCREMENT,
    group_code      VARCHAR(64) NOT NULL COMMENT '分组编码，如 vip_high_value',
    group_name      VARCHAR(128) NOT NULL,
    description     VARCHAR(512),
    group_type      ENUM('static', 'dynamic') NOT NULL DEFAULT 'static',
    filter_rule     JSON COMMENT '动态分组规则',
    member_count    INT NOT NULL DEFAULT 0,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (group_id),
    UNIQUE KEY uk_tenant_code (tenant_id, group_code),
    INDEX idx_tenant (tenant_id)
) ENGINE=InnoDB AUTO_INCREMENT=1001 COMMENT='用户分组（人群包）';

CREATE TABLE IF NOT EXISTS user_group_members (
    tenant_id       BIGINT NOT NULL,
    group_id        BIGINT NOT NULL,
    one_id          BIGINT NOT NULL,
    added_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    source          VARCHAR(32) DEFAULT 'manual',
    PRIMARY KEY (tenant_id, group_id, one_id),
    INDEX idx_one_id (tenant_id, one_id),
    INDEX idx_group (tenant_id, group_id)
) ENGINE=InnoDB COMMENT='分组成员';

-- 预置分组（租户 1001）
INSERT IGNORE INTO user_groups (tenant_id, group_id, group_code, group_name, description, group_type, member_count) VALUES
    (1001, 1001, 'vip_high_value', 'VIP高价值用户', '消费金额高、活跃度高的核心用户', 'static', 1),
    (1001, 1002, 'wechat_users', '微信小程序用户', '来自微信渠道的注册用户', 'static', 1),
    (1001, 1003, 'form_leads', '表单留资用户', '通过表单渠道留资的潜在客户', 'dynamic', 0);

INSERT IGNORE INTO user_group_members (tenant_id, group_id, one_id, source) VALUES
    (1001, 1001, 100001, 'offline'),
    (1001, 1002, 100001, 'offline');

UPDATE user_groups SET member_count = (
    SELECT COUNT(*) FROM user_group_members m WHERE m.group_id = user_groups.group_id
) WHERE tenant_id = 1001;

-- 修复通过管道导入时的中文乱码
UPDATE user_groups SET
    group_name = 'VIP高价值用户',
    description = '消费金额高、活跃度高的核心用户'
WHERE tenant_id = 1001 AND group_code = 'vip_high_value';
UPDATE user_groups SET
    group_name = '微信小程序用户',
    description = '来自微信渠道的注册用户'
WHERE tenant_id = 1001 AND group_code = 'wechat_users';
UPDATE user_groups SET
    group_name = '表单留资用户',
    description = '通过表单渠道留资的潜在客户'
WHERE tenant_id = 1001 AND group_code = 'form_leads';

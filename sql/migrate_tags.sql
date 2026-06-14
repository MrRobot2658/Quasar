USE agenticdmp;

CREATE TABLE IF NOT EXISTS tag_definitions (
    tenant_id       BIGINT NOT NULL,
    tag_id          BIGINT NOT NULL AUTO_INCREMENT,
    parent_id       BIGINT NULL COMMENT 'NULL=一级标签',
    tag_code        VARCHAR(64) NOT NULL,
    tag_name        VARCHAR(128) NOT NULL,
    level           INT NOT NULL DEFAULT 1,
    tag_path        VARCHAR(256) NOT NULL COMMENT '层级路径 value/high_value',
    description     VARCHAR(512),
    sort_order      INT NOT NULL DEFAULT 0,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tag_id),
    UNIQUE KEY uk_tenant_code (tenant_id, tag_code),
    INDEX idx_parent (tenant_id, parent_id),
    INDEX idx_path (tenant_id, tag_path)
) ENGINE=InnoDB AUTO_INCREMENT=2001 COMMENT='用户标签定义（多层级）';

-- 预置多层级标签（租户 1001）
INSERT IGNORE INTO tag_definitions (tenant_id, tag_id, parent_id, tag_code, tag_name, level, tag_path, description, sort_order) VALUES
    (1001, 2001, NULL, 'value', '价值标签', 1, 'value', '用户价值分层', 1),
    (1001, 2002, 2001, 'high_value', '高价值', 2, 'value/high_value', '消费金额高、复购率高', 1),
    (1001, 2003, 2001, 'medium_value', '中价值', 2, 'value/medium_value', '有消费记录的中等用户', 2),
    (1001, 2004, NULL, 'channel', '渠道标签', 1, 'channel', '用户来源渠道', 2),
    (1001, 2005, 2004, 'wechat_user', '微信用户', 2, 'channel/wechat_user', '来自微信小程序/公众号', 1),
    (1001, 2006, 2004, 'form_user', '表单用户', 2, 'channel/form_user', '通过表单留资', 2),
    (1001, 2007, NULL, 'behavior', '行为标签', 1, 'behavior', '用户行为特征', 3),
    (1001, 2008, 2007, 'active', '活跃用户', 2, 'behavior/active', '近30天有活跃行为', 1),
    (1001, 2009, 2007, 'churn_risk', '流失风险', 2, 'behavior/churn_risk', '长期未活跃', 2);

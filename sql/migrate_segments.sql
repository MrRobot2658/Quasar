-- Segment（人群规则）保存（文档 Ch4：用户确认后走现有保存链路）
USE agenticdmp;

CREATE TABLE IF NOT EXISTS segments (
    segment_id      BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id       BIGINT NOT NULL,
    segment_code    VARCHAR(64)  NOT NULL,
    segment_name    VARCHAR(128) NOT NULL,
    base_object     VARCHAR(32)  NOT NULL COMMENT 'user/lead/account/product/store',
    dsl             JSON         NOT NULL COMMENT '候选 DSL Rule',
    estimate        INT          DEFAULT 0 COMMENT '保存时人数预估',
    source          VARCHAR(32)  DEFAULT 'manual' COMMENT 'manual/nl-agent',
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (segment_id),
    UNIQUE KEY uk_tenant_code (tenant_id, segment_code),
    INDEX idx_tenant (tenant_id)
) ENGINE=InnoDB AUTO_INCREMENT=5001 COMMENT='人群 Segment 规则';

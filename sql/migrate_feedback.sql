-- ════════════════════════════════════════════════════════════════════════
-- migrate_feedback.sql · 反馈闭环（CDP 五大 Skill 的「回传 → 迭代」）
-- 详见 docs/13-feedback-loop.md。全部「只做加法」，幂等可重复执行：
--   - 增列走自包含存储过程 _fb_add_col（按 information_schema 判断是否已存在）。
--   - 新表 CREATE TABLE IF NOT EXISTS。
-- 依赖 segments / tag_definitions / broadcasts 已由 segments/tags/modules 迁移建好；
-- apply_migrations.sh 的兜底顺序保证本文件在它们之后执行。
-- ════════════════════════════════════════════════════════════════════════
USE dataagent;

DROP PROCEDURE IF EXISTS _fb_add_col;
DELIMITER //
CREATE PROCEDURE _fb_add_col(IN p_tbl VARCHAR(64), IN p_col VARCHAR(64), IN p_ddl VARCHAR(2048))
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = p_tbl AND COLUMN_NAME = p_col
    ) THEN
        SET @s = CONCAT('ALTER TABLE `', p_tbl, '` ADD COLUMN ', p_ddl);
        PREPARE st FROM @s; EXECUTE st; DEALLOCATE PREPARE st;
    END IF;
END //
DELIMITER ;

-- ── Skill 3 · segments 回写列 ──────────────────────────────────────────────
CALL _fb_add_col('segments','last_engage_id',  "last_engage_id BIGINT NULL COMMENT '最近一次触达 broadcast_id'");
CALL _fb_add_col('segments','sent_count',       "sent_count INT DEFAULT 0 COMMENT '触达发送累计'");
CALL _fb_add_col('segments','opened_count',     "opened_count INT DEFAULT 0");
CALL _fb_add_col('segments','clicked_count',    "clicked_count INT DEFAULT 0");
CALL _fb_add_col('segments','converted_count',  "converted_count INT DEFAULT 0");
CALL _fb_add_col('segments','open_rate',        "open_rate DECIMAL(5,2) DEFAULT 0 COMMENT '打开率%'");
CALL _fb_add_col('segments','click_rate',       "click_rate DECIMAL(5,2) DEFAULT 0 COMMENT '点击率%'");
CALL _fb_add_col('segments','conversion_rate',  "conversion_rate DECIMAL(5,2) DEFAULT 0 COMMENT '转化率%'");
CALL _fb_add_col('segments','quality_score',    "quality_score DECIMAL(4,3) DEFAULT NULL COMMENT '人群质量分 0-1'");
CALL _fb_add_col('segments','feedback_at',      "feedback_at DATETIME NULL COMMENT '最近回写时间'");

-- ── Skill 2 · tag_definitions 回写列 ───────────────────────────────────────
CALL _fb_add_col('tag_definitions','weight',              "weight DECIMAL(4,3) NOT NULL DEFAULT 1.000 COMMENT '标签权重，高转化加权/低效降权'");
CALL _fb_add_col('tag_definitions','select_count',        "select_count INT DEFAULT 0 COMMENT '被圈选累计次数'");
CALL _fb_add_col('tag_definitions','avg_conversion_rate', "avg_conversion_rate DECIMAL(5,2) DEFAULT 0 COMMENT '含该标签人群平均转化率%'");
CALL _fb_add_col('tag_definitions','coverage',            "coverage INT DEFAULT 0 COMMENT '当前覆盖用户数'");
CALL _fb_add_col('tag_definitions','status',              "status ENUM('active','archived') NOT NULL DEFAULT 'active' COMMENT '低效自动归档'");
CALL _fb_add_col('tag_definitions','feedback_at',         "feedback_at DATETIME NULL COMMENT '最近回写时间'");

DROP PROCEDURE IF EXISTS _fb_add_col;

-- ── Skill 2 · 标签圈选频次日志 ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tag_usage_log (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id    BIGINT NOT NULL,
    tag_code     VARCHAR(64) NOT NULL,
    segment_code VARCHAR(64),
    used_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tenant_tag (tenant_id, tag_code),
    INDEX idx_used (tenant_id, used_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='标签被圈选频次日志';

-- ── Skill 1 · 画像字段消费埋点 + 健康度 ────────────────────────────────────
CREATE TABLE IF NOT EXISTS field_usage (
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id      BIGINT NOT NULL,
    object_type    VARCHAR(32) NOT NULL COMMENT 'user/lead/account/...',
    field_name     VARCHAR(64) NOT NULL,
    usage_count    INT DEFAULT 0 COMMENT '被圈选/查询累计次数',
    last_used_at   DATETIME NULL,
    fill_rate      DECIMAL(5,2) DEFAULT NULL COMMENT '填充率%',
    distinct_count INT DEFAULT NULL COMMENT '去重值数',
    recommendation VARCHAR(16) DEFAULT 'keep' COMMENT 'keep/enrich/deprecate',
    scanned_at     DATETIME NULL,
    UNIQUE KEY uk_field (tenant_id, object_type, field_name),
    INDEX idx_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='画像字段消费埋点与健康度';

-- ── Skill 4 · 数据质量巡检结果 + 质量分 ────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_quality_checks (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id    BIGINT NOT NULL,
    object_type  VARCHAR(32) NOT NULL,
    field_name   VARCHAR(64),
    check_type   ENUM('null','duplicate','format') NOT NULL,
    total_rows   INT DEFAULT 0,
    bad_rows     INT DEFAULT 0,
    score        DECIMAL(4,3) DEFAULT 1 COMMENT '0-1，1=无问题',
    severity     ENUM('high','medium','low') DEFAULT 'low',
    sample       JSON COMMENT '命中样例',
    auto_fixable TINYINT DEFAULT 0,
    checked_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_check (tenant_id, object_type, field_name, check_type),
    INDEX idx_tenant (tenant_id),
    INDEX idx_severity (tenant_id, severity)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='数据质量巡检结果与质量分';

-- ── Skill 5 · 分析洞察异常发现 + 归因 + 决策回传 ───────────────────────────
CREATE TABLE IF NOT EXISTS insight_findings (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id       BIGINT NOT NULL,
    finding_type    VARCHAR(32) NOT NULL COMMENT 'sku_drop/cohort_drift',
    dimension       VARCHAR(128) COMMENT '异常落到的维度，如 product=P4001',
    metric          VARCHAR(64)  COMMENT 'gmv/order_count/cohort_share',
    baseline        DECIMAL(16,2) DEFAULT 0 COMMENT '基线值',
    current_value   DECIMAL(16,2) DEFAULT 0 COMMENT '当前值',
    change_pct      DECIMAL(7,2) DEFAULT 0 COMMENT '变化%',
    severity        ENUM('high','medium','low') DEFAULT 'medium',
    attribution     JSON COMMENT '归因明细',
    status          ENUM('open','acknowledged','acted','dismissed') DEFAULT 'open',
    decision        VARCHAR(512) COMMENT '业务决策',
    decision_result VARCHAR(512) COMMENT '结果回传',
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_finding (tenant_id, finding_type, dimension, metric),
    INDEX idx_tenant (tenant_id),
    INDEX idx_status (tenant_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='分析洞察异常发现与决策回传';

-- ── Seed · 触达回执演示数据（租户 1001），让 Skill 3 回写有料可聚合 ─────────
-- 关联 demo segment（若已存在则用其 segment_id；否则先建一个简易 segment）
INSERT IGNORE INTO segments (segment_id, tenant_id, segment_code, segment_name, base_object, dsl, estimate, source)
VALUES (5901, 1001, 'seg_demo_highvalue', '高价值人群(演示)', 'user',
        JSON_OBJECT('object','user','logic','and',
                    'conditions', JSON_ARRAY(JSON_OBJECT('field','tags','op','contains','value','high_value'))),
        3, 'manual');

INSERT IGNORE INTO broadcasts (broadcast_id, tenant_id, broadcast_code, broadcast_name, segment_id, channel_type, subject, estimated_size, status, sent_at)
VALUES (7901, 1001, 'bc_demo_highvalue', '高价值人群唤醒(演示)', 5901, 'email', '专属权益上新', 3, 'sent', NOW());

-- 3 条单发回执：2 打开 / 1 点击（=转化），1 仅送达
INSERT IGNORE INTO broadcast_sends (broadcast_id, tenant_id, one_id, channel_type, sent_at, delivered_at, opened_at, clicked_at, status) VALUES
    (7901, 1001, 100002, 'email', NOW(), NOW(), NOW(), NOW(), 'clicked'),
    (7901, 1001, 100004, 'email', NOW(), NOW(), NOW(), NULL,  'opened'),
    (7901, 1001, 100003, 'email', NOW(), NOW(), NULL,  NULL,  'delivered');

-- ────────────────────────────────────────────────────────────────────────
-- 多对象筛选（文档 V3.0-06 第 3 章 / 图 5·图 6）
-- User 单实体 → User + Lead + Account + Product + Store + object_relations
-- 本地用 MySQL 模拟 Doris 对象表；跨对象 JOIN 经 object_relations 实现。
-- 幂等：CREATE IF NOT EXISTS / INSERT IGNORE
-- ────────────────────────────────────────────────────────────────────────
USE agenticdmp;

-- ── Lead 线索 ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS object_lead (
    tenant_id       BIGINT       NOT NULL,
    lead_id         VARCHAR(64)  NOT NULL,
    lead_name       VARCHAR(128),
    city            VARCHAR(64),
    company_size    INT          DEFAULT 0,
    source          VARCHAR(64)  COMMENT 'campaign/form/ad/referral',
    stage           VARCHAR(32)  COMMENT 'new/qualified/converted/lost',
    properties      JSON,
    create_time     DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, lead_id),
    INDEX idx_city (tenant_id, city),
    INDEX idx_stage (tenant_id, stage)
) ENGINE=InnoDB COMMENT='对象·线索';

-- ── Account 账户 ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS object_account (
    tenant_id       BIGINT       NOT NULL,
    account_id      VARCHAR(64)  NOT NULL,
    name            VARCHAR(128),
    industry        VARCHAR(64)  COMMENT 'manufacturing/tech/retail/finance',
    scale           VARCHAR(32)  COMMENT 'small/medium/large',
    properties      JSON,
    create_time     DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, account_id),
    INDEX idx_industry (tenant_id, industry)
) ENGINE=InnoDB COMMENT='对象·账户(ABM)';

-- ── Product 商品 ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS object_product (
    tenant_id       BIGINT       NOT NULL,
    product_id      VARCHAR(64)  NOT NULL,
    sku             VARCHAR(64),
    category        VARCHAR(64),
    price           DECIMAL(12,2) DEFAULT 0,
    properties      JSON,
    create_time     DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, product_id),
    INDEX idx_category (tenant_id, category)
) ENGINE=InnoDB COMMENT='对象·商品';

-- ── Store 门店 ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS object_store (
    tenant_id       BIGINT       NOT NULL,
    store_id        VARCHAR(64)  NOT NULL,
    store_name      VARCHAR(128),
    region          VARCHAR(64),
    address         VARCHAR(256),
    properties      JSON,
    create_time     DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, store_id),
    INDEX idx_region (tenant_id, region)
) ENGINE=InnoDB COMMENT='对象·门店';

-- ── Order 订单 ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS object_order (
    tenant_id       BIGINT       NOT NULL,
    order_id        VARCHAR(64)  NOT NULL,
    order_no        VARCHAR(64),
    amount          DECIMAL(12,2) DEFAULT 0,
    channel         VARCHAR(32)  COMMENT 'app/web/store',
    status          VARCHAR(32)  COMMENT 'paid/refunded/pending/cancelled',
    properties      JSON,
    create_time     DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, order_id),
    INDEX idx_status (tenant_id, status),
    INDEX idx_channel (tenant_id, channel)
) ENGINE=InnoDB COMMENT='对象·订单';

-- ── object_relations 统一关联存储（文档图 6 关联矩阵）────────────────────
-- 关系方向：src --rel_type--> dst。user 主键存 one_id 的字符串形式。
-- 已定义关系：lead-belongs_to->user、user-owns->account、
--             account-purchased->product、user-visited->store
CREATE TABLE IF NOT EXISTS object_relations (
    tenant_id       BIGINT       NOT NULL,
    src_type        VARCHAR(32)  NOT NULL COMMENT 'user/lead/account/product/store',
    src_id          VARCHAR(64)  NOT NULL,
    rel_type        VARCHAR(32)  NOT NULL COMMENT 'belongs_to/owns/purchased/visited',
    dst_type        VARCHAR(32)  NOT NULL,
    dst_id          VARCHAR(64)  NOT NULL,
    properties      JSON,
    create_time     DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, src_type, src_id, rel_type, dst_type, dst_id),
    -- 正向边
    INDEX idx_fwd (tenant_id, src_type, src_id, rel_type),
    -- 反向边（H2：反向关联查询，如"查门店全部访客"，避免全表扫描）
    INDEX idx_rev (tenant_id, dst_type, dst_id, rel_type)
) ENGINE=InnoDB COMMENT='对象关联（统一边表，正反向索引）';

-- ════════════════════════════════════════════════════════════════════════
-- 模拟数据（租户 1001）
-- 设计用于验证文档示例：「地址在上海、公司规模>500 的线索，且关联用户带 VIP 标签」
--   期望命中：L2001、L2005（L2002 用户非VIP / L2003 非上海 / L2004 规模<500）
-- ════════════════════════════════════════════════════════════════════════

-- 关系目标用户（补充 doris_user_wide，带/不带 vip 标签）
-- 用 UPSERT 确保 demo 用户标签确定性（这几个 one_id 为多对象演示专用）
INSERT INTO doris_user_wide (tenant_id, one_id, phone, channel_count, tags, properties) VALUES
    (1001, 100002, '13800138002', 1, JSON_ARRAY('vip', 'high_value'), JSON_OBJECT('total_amount', 38000)),
    (1001, 100003, '13800138003', 1, JSON_ARRAY('normal'),            JSON_OBJECT('total_amount', 1200)),
    (1001, 100004, '13800138004', 1, JSON_ARRAY('vip'),               JSON_OBJECT('total_amount', 52000))
ON DUPLICATE KEY UPDATE
    tags=VALUES(tags), phone=VALUES(phone), properties=VALUES(properties);

-- Lead
INSERT IGNORE INTO object_lead (tenant_id, lead_id, lead_name, city, company_size, source, stage) VALUES
    (1001, 'L2001', '上海智能科技', '上海', 800,  'campaign', 'qualified'),
    (1001, 'L2002', '上海贸易公司', '上海', 600,  'form',     'new'),
    (1001, 'L2003', '北京制造厂',   '北京', 900,  'campaign', 'qualified'),
    (1001, 'L2004', '上海小微企业', '上海', 300,  'ad',       'new'),
    (1001, 'L2005', '上海大型集团', '上海', 1200, 'campaign', 'qualified');

-- Account
INSERT IGNORE INTO object_account (tenant_id, account_id, name, industry, scale) VALUES
    (1001, 'A3001', '上海制造集团', 'manufacturing', 'large'),
    (1001, 'A3002', '北京科技',     'tech',          'medium');

-- Product
INSERT IGNORE INTO object_product (tenant_id, product_id, sku, category, price) VALUES
    (1001, 'P4001', 'SKU-001', '智能家居', 2999.00),
    (1001, 'P4002', 'SKU-002', '家电',     5999.00);

-- Store
INSERT IGNORE INTO object_store (tenant_id, store_id, store_name, region, address) VALUES
    (1001, 'S5001', '上海旗舰店', '华东', '上海市浦东新区世纪大道100号'),
    (1001, 'S5002', '北京体验店', '华北', '北京市朝阳区建国路88号');

-- Order（演示：含已支付/退款，金额/渠道/状态各异）
INSERT IGNORE INTO object_order (tenant_id, order_id, order_no, amount, channel, status) VALUES
    (1001, 'O90001', 'NO-20260613-001', 1299.00, 'app',   'paid'),
    (1001, 'O90002', 'NO-20260612-002',  459.00, 'web',   'paid'),
    (1001, 'O90003', 'NO-20260530-003',   88.00, 'store', 'refunded');

-- 关联边
INSERT IGNORE INTO object_relations (tenant_id, src_type, src_id, rel_type, dst_type, dst_id) VALUES
    -- lead belongs_to user
    (1001, 'lead', 'L2001', 'belongs_to', 'user', '100002'),
    (1001, 'lead', 'L2002', 'belongs_to', 'user', '100003'),
    (1001, 'lead', 'L2003', 'belongs_to', 'user', '100004'),
    (1001, 'lead', 'L2004', 'belongs_to', 'user', '100002'),
    (1001, 'lead', 'L2005', 'belongs_to', 'user', '100004'),
    -- user owns account
    (1001, 'user', '100002', 'owns', 'account', 'A3001'),
    (1001, 'user', '100004', 'owns', 'account', 'A3002'),
    -- account purchased product
    (1001, 'account', 'A3001', 'purchased', 'product', 'P4001'),
    (1001, 'account', 'A3001', 'purchased', 'product', 'P4002'),
    -- user visited store
    (1001, 'user', '100002', 'visited', 'store', 'S5001'),
    (1001, 'user', '100004', 'visited', 'store', 'S5002'),
    -- user placed order
    (1001, 'user', '100002', 'placed', 'order', 'O90001'),
    (1001, 'user', '100002', 'placed', 'order', 'O90002'),
    (1001, 'user', '100004', 'placed', 'order', 'O90003'),
    -- order contains product
    (1001, 'order', 'O90001', 'contains', 'product', 'P4001'),
    (1001, 'order', 'O90002', 'contains', 'product', 'P4002'),
    (1001, 'order', 'O90003', 'contains', 'product', 'P4001');

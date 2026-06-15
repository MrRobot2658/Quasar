-- 智能助手聊天记录（按用户存储）。每条消息一行，user_id 为登录用户。
USE agenticdatahub;

CREATE TABLE IF NOT EXISTS assistant_messages (
    id          BIGINT       NOT NULL AUTO_INCREMENT,
    tenant_id   BIGINT       NOT NULL,
    user_id     BIGINT       NOT NULL,
    role        VARCHAR(16)  NOT NULL COMMENT 'user/assistant',
    content     MEDIUMTEXT   NOT NULL,
    agent       VARCHAR(32)  NULL COMMENT '处理该回复的智能体 key',
    created_at  DATETIME     DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_user (tenant_id, user_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

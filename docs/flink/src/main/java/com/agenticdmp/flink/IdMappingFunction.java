package com.agenticdmp.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Set;

/**
 * Job-1 核心：实时 ID-Mapping
 *
 * 逻辑与 services/id-mapping/main.py IdMappingService.process_event() 对齐：
 *   Redis 热层 → MySQL/Doris 冷层 → link_keys 跨渠道关联 → create/link/merge
 */
public class IdMappingFunction extends KeyedProcessFunction<String, UserEvent, EnrichedEvent> {

    private static final Set<String> CHANNEL_TYPES = Set.of(
            "wechat_openid", "wechat_unionid", "wework_extid",
            "form_id", "phone", "email", "device"
    );

    private final JobConfig config;
    private transient Jedis jedis;
    private transient Connection mysqlConn;
    private transient ObjectMapper mapper;

    public IdMappingFunction(JobConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        mapper = new ObjectMapper();
        jedis = new Jedis(config.redisHost, config.redisPort);
        mysqlConn = DriverManager.getConnection(
                config.mysqlUrl, config.mysqlUser, config.mysqlPassword);
        mysqlConn.setAutoCommit(true);
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) jedis.close();
        if (mysqlConn != null) mysqlConn.close();
    }

    @Override
    public void processElement(UserEvent event, Context ctx, Collector<EnrichedEvent> out) throws Exception {
        long tenantId = event.tenantId;
        String channelType = event.channelType;
        String channelId = event.channelId;

        // 1. Redis 热层
        Long oneId = getOneIdFromRedis(tenantId, channelType, channelId);
        String action = "hit_cache";

        // 2. Redis miss → MySQL 冷层
        if (oneId == null) {
            oneId = queryOneIdFromMysql(tenantId, channelType, channelId);
            if (oneId != null) {
                cacheMapping(tenantId, channelType, channelId, oneId);
                action = "hit_mysql";
            }
        }

        // 3. link_keys 跨渠道关联
        Long linkedOneId = null;
        if (event.linkKeys != null && !event.linkKeys.isEmpty()) {
            linkedOneId = queryOneIdByLinkKeys(tenantId, event.linkKeys);
        }

        if (oneId != null && linkedOneId != null && !oneId.equals(linkedOneId)) {
            oneId = mergeOneIds(tenantId, oneId, linkedOneId);
            action = "merge";
            logMerge(tenantId, event.eventId, "merge", oneId, channelType, channelId, linkedOneId);
        } else if (oneId == null && linkedOneId != null) {
            oneId = linkedOneId;
            insertMapping(tenantId, channelType, channelId, oneId, "link");
            cacheMapping(tenantId, channelType, channelId, oneId);
            action = "link";
            logMerge(tenantId, event.eventId, "link", oneId, channelType, channelId, linkedOneId);
        } else if (oneId == null) {
            oneId = generateOneId(tenantId);
            insertMapping(tenantId, channelType, channelId, oneId, "realtime");
            cacheMapping(tenantId, channelType, channelId, oneId);
            action = "create";
            logMerge(tenantId, event.eventId, "create", oneId, channelType, channelId, null);
        }

        // 4. 同步 link_keys 到映射表
        if (event.linkKeys != null) {
            for (Map.Entry<String, String> entry : event.linkKeys.entrySet()) {
                String lkType = entry.getKey();
                String lkId = entry.getValue();
                if (CHANNEL_TYPES.contains(lkType) && lkId != null && !lkId.isEmpty()) {
                    insertMapping(tenantId, lkType, lkId, oneId, "link");
                    cacheMapping(tenantId, lkType, lkId, oneId);
                }
            }
        }

        out.collect(EnrichedEvent.from(event, oneId, action, linkedOneId));
    }

    // ── Redis ──────────────────────────────────────────────────────────────

    private String channelKey(long tenantId, String channelType, String channelId) {
        return "channel:" + tenantId + ":" + channelType + ":" + channelId;
    }

    private String uidChannelsKey(long tenantId, long oneId) {
        return "uid:" + tenantId + ":" + oneId + ":channels";
    }

    private Long getOneIdFromRedis(long tenantId, String channelType, String channelId) {
        String val = jedis.get(channelKey(tenantId, channelType, channelId));
        return val != null ? Long.parseLong(val) : null;
    }

    private void cacheMapping(long tenantId, String channelType, String channelId, long oneId) {
        jedis.setex(channelKey(tenantId, channelType, channelId), config.redisTtlSeconds, String.valueOf(oneId));
        jedis.hset(uidChannelsKey(tenantId, oneId), channelType, channelId);
        jedis.expire(uidChannelsKey(tenantId, oneId), config.redisTtlSeconds);
    }

    // ── MySQL 冷层 ────────────────────────────────────────────────────────

    private Long queryOneIdFromMysql(long tenantId, String channelType, String channelId) throws Exception {
        try (PreparedStatement ps = mysqlConn.prepareStatement(
                "SELECT one_id FROM id_mapping WHERE tenant_id=? AND channel_type=? AND channel_id=?")) {
            ps.setLong(1, tenantId);
            ps.setString(2, channelType);
            ps.setString(3, channelId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong("one_id") : null;
            }
        }
    }

    private Long queryOneIdByLinkKeys(long tenantId, Map<String, String> linkKeys) throws Exception {
        for (Map.Entry<String, String> entry : linkKeys.entrySet()) {
            if (!CHANNEL_TYPES.contains(entry.getKey()) || entry.getValue() == null) continue;
            Long id = getOneIdFromRedis(tenantId, entry.getKey(), entry.getValue());
            if (id != null) return id;
            id = queryOneIdFromMysql(tenantId, entry.getKey(), entry.getValue());
            if (id != null) {
                cacheMapping(tenantId, entry.getKey(), entry.getValue(), id);
                return id;
            }
        }
        return null;
    }

    private long generateOneId(long tenantId) throws Exception {
        try (PreparedStatement ps = mysqlConn.prepareStatement(
                "INSERT INTO one_id_sequence (tenant_id, next_id) VALUES (?, 100000) " +
                "ON DUPLICATE KEY UPDATE next_id = LAST_INSERT_ID(next_id + 1)")) {
            ps.setLong(1, tenantId);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = mysqlConn.prepareStatement("SELECT LAST_INSERT_ID() AS one_id");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong("one_id");
        }
    }

    private void insertMapping(long tenantId, String channelType, String channelId,
                               long oneId, String source) throws Exception {
        try (PreparedStatement ps = mysqlConn.prepareStatement(
                "INSERT INTO id_mapping (tenant_id, channel_type, channel_id, one_id, source) " +
                "VALUES (?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE one_id=VALUES(one_id), source=VALUES(source), update_time=NOW()")) {
            ps.setLong(1, tenantId);
            ps.setString(2, channelType);
            ps.setString(3, channelId);
            ps.setLong(4, oneId);
            ps.setString(5, source);
            ps.executeUpdate();
        }
    }

    private long mergeOneIds(long tenantId, long fromId, long toId) throws Exception {
        long primary = Math.min(fromId, toId);
        long secondary = Math.max(fromId, toId);
        if (primary == secondary) return primary;

        try (PreparedStatement ps = mysqlConn.prepareStatement(
                "UPDATE id_mapping SET one_id=?, source='merge', update_time=NOW() " +
                "WHERE tenant_id=? AND one_id=?")) {
            ps.setLong(1, primary);
            ps.setLong(2, tenantId);
            ps.setLong(3, secondary);
            ps.executeUpdate();
        }

        // 重新缓存 primary 下所有映射
        try (PreparedStatement ps = mysqlConn.prepareStatement(
                "SELECT channel_type, channel_id FROM id_mapping WHERE tenant_id=? AND one_id=?")) {
            ps.setLong(1, tenantId);
            ps.setLong(2, primary);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cacheMapping(tenantId, rs.getString("channel_type"), rs.getString("channel_id"), primary);
                }
            }
        }
        return primary;
    }

    private void logMerge(long tenantId, String eventId, String action, long oneId,
                          String channelType, String channelId, Long linkedOneId) throws Exception {
        try (PreparedStatement ps = mysqlConn.prepareStatement(
                "INSERT INTO merge_log (tenant_id, event_id, action, one_id, channel_type, channel_id, linked_one_id) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            ps.setLong(1, tenantId);
            ps.setString(2, eventId);
            ps.setString(3, action);
            ps.setLong(4, oneId);
            ps.setString(5, channelType);
            ps.setString(6, channelId);
            if (linkedOneId != null) ps.setLong(7, linkedOneId);
            else ps.setNull(7, java.sql.Types.BIGINT);
            ps.executeUpdate();
        }
    }

    /** Job 运行参数 */
    public static class JobConfig {
        public String redisHost = "redis";
        public int redisPort = 6379;
        public int redisTtlSeconds = 2_592_000;
        public String mysqlUrl = "jdbc:mysql://mysql:3306/agenticdmp";
        public String mysqlUser = "agenticdmp";
        public String mysqlPassword = "agenticdmp123";
    }
}

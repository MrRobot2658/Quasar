package com.agenticdmp.flink;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * ID-Mapping 富化后事件，写入 enriched-{tenant_id}-events
 * 供 Job-2 画像聚合 / Job-3 宽表打宽消费
 */
public class EnrichedEvent {

    @JsonProperty("event_id")
    public String eventId;

    @JsonProperty("tenant_id")
    public long tenantId;

    @JsonProperty("one_id")
    public long oneId;

    @JsonProperty("channel_type")
    public String channelType;

    @JsonProperty("channel_id")
    public String channelId;

    @JsonProperty("event_type")
    public String eventType;

    @JsonProperty("event_time")
    public String eventTime;

    /** create / link / merge / hit_cache / hit_mysql */
    @JsonProperty("action")
    public String action;

    @JsonProperty("linked_one_id")
    public Long linkedOneId;

    @JsonProperty("properties")
    public Map<String, Object> properties = new HashMap<>();

    @JsonProperty("processed_at")
    public String processedAt;

    public static EnrichedEvent from(UserEvent e, long oneId, String action, Long linkedOneId) {
        EnrichedEvent out = new EnrichedEvent();
        out.eventId = e.eventId;
        out.tenantId = e.tenantId;
        out.oneId = oneId;
        out.channelType = e.channelType;
        out.channelId = e.channelId;
        out.eventType = e.eventType;
        out.eventTime = e.eventTime;
        out.action = action;
        out.linkedOneId = linkedOneId;
        out.properties = e.properties;
        out.processedAt = java.time.Instant.now().toString();
        return out;
    }
}

package com.agenticdmp.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 入站用户事件（微信 / 企微 / 表单）
 * 对应 services/id-mapping/main.py UserEvent
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserEvent {

    @JsonProperty("event_id")
    public String eventId;

    @JsonProperty("tenant_id")
    public long tenantId;

    @JsonProperty("channel_type")
    public String channelType;

    @JsonProperty("channel_id")
    public String channelId;

    @JsonProperty("event_type")
    public String eventType = "page_view";

    @JsonProperty("event_time")
    public String eventTime;

    @JsonProperty("link_keys")
    public Map<String, String> linkKeys = new HashMap<>();

    @JsonProperty("properties")
    public Map<String, Object> properties = new HashMap<>();

    public String identityKey() {
        return tenantId + ":" + channelType + ":" + channelId;
    }
}

package com.agenticdmp.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Job-1: 实时 ID-Mapping
 *
 * Kafka(user-events) → IdMappingFunction → Kafka(enriched-events)
 *
 * 提交示例:
 *   flink run -c com.agenticdmp.flink.IdMappingJob target/user-profile-flink-jobs-1.0.0.jar \
 *     --tenant-id 1001 \
 *     --kafka-bootstrap kafka:9092 \
 *     --source-topic tenant-1001-events \
 *     --sink-topic enriched-1001-events
 */
public class IdMappingJob {

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.parse(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);
        env.setParallelism(params.parallelism);

        // ── Kafka Source ──────────────────────────────────────────────────
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(params.kafkaBootstrap)
                .setTopics(params.sourceTopic)
                .setGroupId("id-mapping-" + params.tenantId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<UserEvent> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-user-events")
                .process(new JsonToUserEventFunction())
                .filter(e -> e.tenantId == params.tenantId)
                .name("parse-user-event");

        // ── ID-Mapping ────────────────────────────────────────────────────
        IdMappingFunction.JobConfig jobConfig = new IdMappingFunction.JobConfig();
        jobConfig.redisHost = params.redisHost;
        jobConfig.redisPort = params.redisPort;
        jobConfig.mysqlUrl = params.mysqlUrl;
        jobConfig.mysqlUser = params.mysqlUser;
        jobConfig.mysqlPassword = params.mysqlPassword;

        DataStream<EnrichedEvent> enriched = events
                .keyBy(UserEvent::identityKey)
                .process(new IdMappingFunction(jobConfig))
                .name("id-mapping");

        // ── Kafka Sink ────────────────────────────────────────────────────
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(params.kafkaBootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(params.sinkTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        enriched
                .process(new EnrichedEventToJsonFunction())
                .sinkTo(sink)
                .name("kafka-enriched-events");

        env.execute("id-mapping-" + params.tenantId);
    }

    // ── 序列化辅助 ────────────────────────────────────────────────────────

    static class JsonToUserEventFunction extends ProcessFunction<String, UserEvent> {
        private transient ObjectMapper mapper;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            mapper = new ObjectMapper();
        }

        @Override
        public void processElement(String value, Context ctx, Collector<UserEvent> out) throws Exception {
            out.collect(mapper.readValue(value, UserEvent.class));
        }
    }

    static class EnrichedEventToJsonFunction extends ProcessFunction<EnrichedEvent, String> {
        private transient ObjectMapper mapper;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            mapper = new ObjectMapper();
        }

        @Override
        public void processElement(EnrichedEvent value, Context ctx, Collector<String> out) throws Exception {
            out.collect(mapper.writeValueAsString(value));
        }
    }

    // ── 命令行参数 ────────────────────────────────────────────────────────

    static class JobParams {
        long tenantId = 1001;
        String kafkaBootstrap = "kafka:9092";
        String sourceTopic = "tenant-1001-events";
        String sinkTopic = "enriched-1001-events";
        String redisHost = "redis";
        int redisPort = 6379;
        String mysqlUrl = "jdbc:mysql://mysql:3306/agenticdmp";
        String mysqlUser = "agenticdmp";
        String mysqlPassword = "agenticdmp123";
        int parallelism = 4;

        static JobParams parse(String[] args) {
            JobParams p = new JobParams();
            for (int i = 0; i < args.length - 1; i++) {
                switch (args[i]) {
                    case "--tenant-id":        p.tenantId = Long.parseLong(args[++i]); break;
                    case "--kafka-bootstrap":  p.kafkaBootstrap = args[++i]; break;
                    case "--source-topic":     p.sourceTopic = args[++i]; break;
                    case "--sink-topic":       p.sinkTopic = args[++i]; break;
                    case "--redis-host":       p.redisHost = args[++i]; break;
                    case "--redis-port":       p.redisPort = Integer.parseInt(args[++i]); break;
                    case "--mysql-url":        p.mysqlUrl = args[++i]; break;
                    case "--mysql-user":       p.mysqlUser = args[++i]; break;
                    case "--mysql-password":   p.mysqlPassword = args[++i]; break;
                    case "--parallelism":      p.parallelism = Integer.parseInt(args[++i]); break;
                }
            }
            return p;
        }
    }
}

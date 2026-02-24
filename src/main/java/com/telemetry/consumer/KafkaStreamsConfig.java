package com.telemetry.consumer;

import com.telemetry.model.TelemetrySignal;
import com.telemetry.model.VehicleState;
import com.telemetry.serde.JsonSerde;
import com.telemetry.topology.MongoSinkProcessor;
import com.telemetry.topology.MongoHistorySinkProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Wires up the Kafka Streams topology:
 *
 *  ┌────────────────────┐
 *  │  vehicle-signals    │   Kafka topic (input)
 *  │  (TelemetrySignal)  │
 *  └────────┬───────────┘
 *           │
 *     selectKey(did)
 *           │
 *      ┌────┴─────────────────────────┐
 *      │                              │
 *  BRANCH A                       BRANCH B
 *  (raw history)              (aggregated state)
 *      │                              │
 *  process →                    groupByKey(did)
 *  MongoHistorySinkProc.              │
 *      │                    aggregate → VehicleState
 *      │                      (out-of-order protection)
 *      │                              │
 *      │                        toStream()
 *      │                              │
 *      │                    process → MongoSinkProc.
 *      │                              │
 *  ┌───▼──────────────┐   ┌──────────▼──────────┐
 *  │  MongoDB           │   │  MongoDB             │
 *  │  telemetry_history │   │  vehicle_telemetry   │
 *  │  (every signal)    │   │  (one doc per did)   │
 *  └────────────────────┘   └─────────────────────┘
 */
@Configuration
@EnableKafkaStreams
@Slf4j
public class KafkaStreamsConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.security.protocol}")
    private String securityProtocol;

    @Value("${spring.kafka.properties.sasl.mechanism}")
    private String saslMechanism;

    @Value("${spring.kafka.properties.sasl.jaas.config}")
    private String saslJaasConfig;

    @Value("${app.kafka.application-id}")
    private String applicationId;

    @Value("${app.kafka.input-topic}")
    private String inputTopic;

    @Value("${app.kafka.state-store-name}")
    private String stateStoreName;

    @Value("${app.mongodb.collection}")
    private String mongoCollection;

    @Value("${app.mongodb.history-collection}")
    private String historyCollection;

    private final JsonSerde<TelemetrySignal> signalSerde = new JsonSerde<>(TelemetrySignal.class);
    private final JsonSerde<VehicleState> stateSerde = new JsonSerde<>(VehicleState.class);

    // KStreams default config

    @Bean(name = "defaultKafkaStreamsConfig")
    public KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // ── Confluent Cloud SASL/SSL authentication ─────────────────────
        props.put("security.protocol", securityProtocol);
        props.put("sasl.mechanism", saslMechanism);
        props.put("sasl.jaas.config", saslJaasConfig);

        // Processing guarantee
        // Note: EXACTLY_ONCE_V2 requires a Dedicated Confluent Cloud cluster.
        //       Use AT_LEAST_ONCE for Basic/Standard clusters.
        //       The MongoDB upsert is idempotent, so duplicates are harmless.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);

        // Commit interval – controls how often state + offsets are flushed
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // Number of standby replicas for fault tolerance
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);

        // Replication factor for internal topics (changelogs, repartition)
        // Confluent Cloud requires 3 for Basic/Standard clusters
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);

        return new KafkaStreamsConfiguration(props);
    }

    // Topology

    @Bean
    public KStream<String, VehicleState> vehicleTelemetryStream(
            StreamsBuilder builder,
            MongoTemplate mongoTemplate) {

        // 1: Consume raw signals from the input topic
        KStream<String, TelemetrySignal> signalStream = builder
                .stream(inputTopic, Consumed.with(Serdes.String(), signalSerde));

        // 2: Re-key by device ID (did) so all signals for a vehicle
        //     land in the same partition → same state store shard
        KStream<String, TelemetrySignal> keyedStream = signalStream
                .selectKey((ignoredKey, signal) -> signal.getDid());

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // BRANCH A: Raw history sink
        // Inserts every signal as-is into telemetry_history collection
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        keyedStream.process(
                () -> new MongoHistorySinkProcessor(mongoTemplate, historyCollection)
        );

        // BRANCH B: Aggregated state sink (existing logic) [processing the raw signal]

        // 3: Group by the new key (did)
        KGroupedStream<String, TelemetrySignal> grouped = keyedStream
                .groupByKey(Grouped.with(Serdes.String(), signalSerde));

        // 4: Aggregate into VehicleState.
        //     The Aggregator calls VehicleState.merge() which enforces
        //     the per-signal eventTime ordering rule.
        KTable<String, VehicleState> vehicleTable = grouped.aggregate(
                VehicleState::new,                              // initializer
                (did, signal, state) -> state.merge(signal),    // aggregator
                Materialized.<String, VehicleState, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>
                        as(stateStoreName)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(stateSerde)
        );

        // 5: Convert table changes back to a stream and sink to MongoDB
        KStream<String, VehicleState> outputStream = vehicleTable.toStream();

        outputStream.process(
                () -> new MongoSinkProcessor(mongoTemplate, mongoCollection)
        );

        return outputStream;
    }

    @Bean
    public String mongoCollectionName() {
        return mongoCollection;
    }
}

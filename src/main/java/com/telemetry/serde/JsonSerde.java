package com.telemetry.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Reusable Jackson-based Serde for Kafka Streams.
 * Handles serialization / deserialization of any POJO to/from JSON bytes.
 *
 * Usage:
 *   Serde<TelemetrySignal> signalSerde = new JsonSerde<>(TelemetrySignal.class);
 *   Serde<VehicleState> stateSerde = new JsonSerde<>(VehicleState.class);
 */
public class JsonSerde<T> implements Serde<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Class<T> targetType;

    public JsonSerde(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing " + targetType.getSimpleName(), e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null || bytes.length == 0) return null;
            try {
                return MAPPER.readValue(bytes, targetType);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing " + targetType.getSimpleName(), e);
            }
        };
    }
}

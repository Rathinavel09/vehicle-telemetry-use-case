package com.telemetry.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;

/**
 * MongoDB document for the raw telemetry history collection.
 * Stores every signal as-is (no aggregation, no out-of-order filtering).
 *
 * Output format:
 * {
 *   "did": "981",
 *   "timestamp": "1761559635000",
 *   "name": "odometer",
 *   "value_type": "0",
 *   "float_value": 101.50,
 *   "string_value": "",
 *   "int_value": "0",
 *   "eventTime": "2025-10-27T10:07:15.000Z",
 *   "receivedTime": "2025-10-27T10:07:15.875Z",
 *   "dbIngestion_time": "2025-10-27T10:07:16.334Z"
 * }
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "telemetry_history")
public class TelemetryHistoryDocument {

    @Id
    private String id;

    private String did;

    private String timestamp;

    private String name;

    @Field("value_type")
    private String valueType;

    @Field("float_value")
    private double floatValue;

    @Field("string_value")
    private String stringValue;

    @Field("int_value")
    private Object intValue;

    private String eventTime;

    private String receivedTime;
    private String publishTime;

    @Field("dbIngestion_time")
    private String dbIngestionTime;

    // ── Factory ─────────────────────────────────────────────────────────

    public static TelemetryHistoryDocument fromSignal(TelemetrySignal signal) {
        return TelemetryHistoryDocument.builder()
                .did(signal.getDid())
                .timestamp(signal.getTimestamp())
                .name(signal.getName())
                .valueType(signal.getValueType())
                .floatValue(signal.getFloatValue())
                .stringValue(signal.getStringValue())
                .intValue(signal.getIntValue())
                .eventTime(signal.getEventTime())
                .receivedTime(signal.getReceivedTime())
                .publishTime(signal.getPublishTime())
                .dbIngestionTime(Instant.now().toString())
                .build();
    }
}

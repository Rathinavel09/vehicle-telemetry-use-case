package com.telemetry.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a single telemetry signal as received from the Kafka topic.
 *
 * Example payload:
 * {
 *   "did": "981",
 *   "timestamp": "1761559635000",
 *   "name": "odometer",
 *   "value_type": "0",          // 0 = float, 1 = int, 2 = string
 *   "float_value": 101.50,
 *   "string_value": "",
 *   "int_value": "0",
 *   "eventTime": "2025-10-27T10:07:15.000Z",
 *   "receivedTime": "2025-10-27T10:07:15.875Z"
 * }
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TelemetrySignal {

    /** Device ID – groups all signals belonging to one vehicle */
    private String did;

    /** Epoch millis from the vehicle ECU */
    private String timestamp;

    /** Signal name: odometer, soc, speed, ignition_status, etc. */
    private String name;

    /** Discriminator: 0 → float_value, 1 → int_value, 2 → string_value */
    @JsonProperty("value_type")
    private String valueType;

    @JsonProperty("float_value")
    private double floatValue;

    @JsonProperty("string_value")
    private String stringValue;

    @JsonProperty("int_value")
    private Object intValue;   // comes as String or Number depending on payload

    /** timestamp from the vehicle/gateway */
    private String eventTime;

    /** timestamp when the platform received the signal */
    private String receivedTime;

    /** Timestamp when the payload was published */
    private String publishTime;

    // ── Helpers ──────────────────────────────────────────────────────────

    /**
     * Returns the resolved numeric value based on value_type.
     *  0 → float_value
     *  1 → int_value (parsed as double for uniform handling)
     */
    public double getResolvedValue() {
        if ("0".equals(valueType)) {
            return floatValue;
        }
        // int_value may arrive as String "0" or as Number 100
        if (intValue instanceof Number num) {
            return num.doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(intValue));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}

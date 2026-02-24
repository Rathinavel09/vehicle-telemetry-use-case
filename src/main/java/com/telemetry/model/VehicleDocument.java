package com.telemetry.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * MongoDB document representing the latest aggregated telemetry state
 * for a single vehicle. One document per {@code did}.
 *
 * Output format:
 * {
 *   "did": "981",
 *   "eventTime": "2025-10-27T10:07:16.985Z",
 *   "odo": 101.50,
 *   "soc": 100,
 *   "speed": 25,
 *   "ignition": 1,
 *   "ingestion_time": "2025-10-27T10:07:18.363Z"
 * }
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "#{@mongoCollectionName}")
public class VehicleDocument {

    @Id
    private String did;

    /** Max eventTime across all signals */
    @Field("eventTime")
    private String eventTime;

    /** Odometer reading (km) */
    @Field("odo")
    private Double odo;

    /** State of Charge (%) */
    @Field("soc")
    private Integer soc;

    /** Vehicle speed (km/h) */
    @Field("speed")
    private Integer speed;

    /** Ignition status (1 = ON, 0 = OFF) */
    @Field("ignition")
    private Integer ignition;

    /** Timestamp when the payload was published */
    @Field("publishTime")
    private String publishTime;

    /** Timestamp when this document was written/updated in MongoDB */
    @Field("ingestion_time")
    private String ingestionTime;

    // ── Factory ─────────────────────────────────────────────────────────

    /**
     * Builds a {@link VehicleDocument} from the KStreams aggregated
     * {@link VehicleState}.
     */
    public static VehicleDocument fromState(VehicleState state) {
        return VehicleDocument.builder()
                .did(state.getDid())
                .eventTime(state.getLatestEventTime())
                .odo(state.getSignalValue("odometer"))
                .soc(toInt(state.getSignalValue("soc")))
                .speed(toInt(state.getSignalValue("speed")))
                .ignition(toInt(state.getSignalValue("ignition_status")))
                .ingestionTime(java.time.Instant.now().toString())
                .publishTime(state.getPublishTime())
                .build();
    }

    private static Integer toInt(Double val) {
        return val != null ? val.intValue() : null;
    }
}

package com.telemetry.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregated state for a single vehicle (did).
 *
 * Maintains a map of signal-name → {@link SignalState}, where each signal's
 * value is only updated if the incoming eventTime is strictly greater than
 * the currently stored eventTime for that signal.
 *
 * This is the value type stored in the KStreams state store and is serialized
 * to/from JSON via Jackson.
 */
@Data
@NoArgsConstructor
@Slf4j
public class VehicleState {

    /** Device ID */
    private String did;

    /**
     * Per-signal state: key = signal name (odometer, soc, speed, ignition_status …)
     * value = latest accepted value + its eventTime
     */
    private Map<String, SignalState> signals = new HashMap<>();

    /**
     * The maximum eventTime seen across all signals.
     * Used as the document-level eventTime in MongoDB.
     */
    private String latestEventTime;

    /** Timestamp when the payload was published */
    private String publishTime;

    // ── Core aggregation logic ──────────────────────────────────────────

    /**
     * Merges an incoming telemetry signal into this vehicle's state.
     *
     * Out-of-order rule:  if the incoming signal's eventTime is ≤ the
     * eventTime already recorded for that specific signal, the value is
     * dropped (the existing, newer value wins).
     *
     * @param signal incoming telemetry signal
     * @return this (for fluent chaining)
     */
    public VehicleState merge(TelemetrySignal signal) {
        this.did = signal.getDid();
        this.publishTime = signal.getPublishTime();

        String signalName = signal.getName();
        String incomingEventTime = signal.getEventTime();
        double incomingValue = signal.getResolvedValue();

        SignalState existing = signals.get(signalName);

        if (existing == null) {
            // First time seeing this signal → accept unconditionally
            signals.put(signalName, new SignalState(incomingValue, incomingEventTime));
            log.debug("did={} | signal={} | INIT value={} eventTime={}",
                    did, signalName, incomingValue, incomingEventTime);
        } else {
            // Compare eventTimes
            if (isNewer(incomingEventTime, existing.getEventTime())) {
                existing.setValue(incomingValue);
                existing.setEventTime(incomingEventTime);
                log.debug("did={} | signal={} | UPDATED value={} eventTime={}",
                        did, signalName, incomingValue, incomingEventTime);
            } else {
                log.debug("did={} | signal={} | SKIPPED (out-of-order) incoming={} existing={}",
                        did, signalName, incomingEventTime, existing.getEventTime());
            }
        }

        // Track the overall latest eventTime for the document
        if (latestEventTime == null || isNewer(incomingEventTime, latestEventTime)) {
            latestEventTime = incomingEventTime;
        }

        return this;
    }


    /**
     * Returns true if {@code candidate} is strictly after {@code baseline}.
     * Uses {@link Instant} parsing for robust comparison.
     */
    private boolean isNewer(String candidate, String baseline) {
        try {
            Instant candidateInstant = Instant.parse(candidate);
            Instant baselineInstant = Instant.parse(baseline);
            return candidateInstant.isAfter(baselineInstant);
        } catch (Exception e) {
            // Fallback comparison if parsing fails
            return candidate.compareTo(baseline) > 0;
        }
    }

    // ── Convenience accessors for building the Mongo document ───────────

    public Double getSignalValue(String signalName) {
        SignalState s = signals.get(signalName);
        return s != null ? s.getValue() : null;
    }

    public String getSignalEventTime(String signalName) {
        SignalState s = signals.get(signalName);
        return s != null ? s.getEventTime() : null;
    }
}

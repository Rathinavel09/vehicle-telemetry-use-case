package com.telemetry.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Tracks the latest accepted value for a single signal along with its
 * eventTime. Used inside {@link VehicleState} to decide whether an incoming
 * signal should be accepted or rejected (out-of-order protection).
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SignalState {

    /** The accepted value of this signal */
    private double value;

    /** The eventTime (ISO-8601) of the accepted value */
    private String eventTime;
}

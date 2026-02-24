# Vehicle Telemetry KStreams Aggregator

A **Spring Boot + Kafka Streams** application that consumes individual vehicle telemetry signals from a Kafka topic, aggregates them into a single document per vehicle (`did`), and upserts to MongoDB — with **per-signal out-of-order event rejection** based on `eventTime`.


## Out-of-Order Handling

Each signal's value is tracked independently with its own `eventTime`. When a new signal arrives:
| `incoming.eventTime > stored.eventTime` | **Accept** — update the signal value |
| `incoming.eventTime ≤ stored.eventTime` | **Reject** — keep existing value |

**Example with payloads:**

```
payload-3: speed=25,  eventTime=10:07:16.061Z  → ACCEPTED (first speed)
payload-7: speed=65,  eventTime=10:06:36.663Z  → REJECTED (older than 10:07:16)
                                                   speed stays at 25 
```


## Adding New Signals

To support a new signal (e.g., `battery_temp`):

1. No code changes needed for the KStreams aggregation — `VehicleState.merge()` handles any signal name dynamically.
2. Add the field to `VehicleDocument.java` and update `fromState()`.
3. Add the field to the MongoDB upsert in `MongoSinkProcessor`.

## Key Design Decisions
1. **KStreams state store** (RocksDB) holds the aggregated `VehicleState` per `did` — survives restarts via changelog topic.
2. **Exactly-once semantics** (`EXACTLY_ONCE_V2`) ensures no duplicate writes to MongoDB on rebalance/restart.
3. **Upsert to MongoDB** — the processor does atomic upsert, so the document is always the latest aggregated state.
4. **Per-signal eventTime tracking** — each signal inside `VehicleState` has its own `SignalState.eventTime`, enabling independent out-of-order rejection.

package com.telemetry.topology;

import com.telemetry.model.TelemetryHistoryDocument;
import com.telemetry.model.TelemetrySignal;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * KStreams Processor that inserts every raw telemetry signal into the
 * {@code telemetry_history} MongoDB collection.
 *
 * No aggregation, no deduplication — every signal is persisted as a
 * separate document with a {@code dbIngestion_time} stamp.
 */
@Slf4j
public class MongoHistorySinkProcessor implements Processor<String, TelemetrySignal, Void, Void> {

    private final MongoTemplate mongoTemplate;
    private final String collectionName;

    public MongoHistorySinkProcessor(MongoTemplate mongoTemplate, String collectionName) {
        this.mongoTemplate = mongoTemplate;
        this.collectionName = collectionName;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
    }

    @Override
    public void process(Record<String, TelemetrySignal> record) {
        TelemetrySignal signal = record.value();
        if (signal == null) {
            return;
        }

        try {
            TelemetryHistoryDocument doc = TelemetryHistoryDocument.fromSignal(signal);
            mongoTemplate.insert(doc, collectionName);

            log.info("did={} | signal={} | Inserted to {} | eventTime={}",
                    signal.getDid(), signal.getName(), collectionName, signal.getEventTime());

        } catch (Exception e) {
            log.error("did={} | signal={} | Failed to insert to {}",
                    signal.getDid(), signal.getName(), collectionName, e);
        }
    }

    @Override
    public void close() {
        // no-op
    }
}

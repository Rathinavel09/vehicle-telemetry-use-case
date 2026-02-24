package com.telemetry.topology;

import com.telemetry.model.VehicleDocument;
import com.telemetry.model.VehicleState;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

/**
 * KStreams Processor that writes the aggregated {@link VehicleState} to
 * MongoDB as a single document per {@code did}.
 *
 * Uses MongoDB upsert: if the document exists it gets updated, otherwise
 * a new one is inserted.
 */
@Slf4j
public class MongoSinkProcessor implements Processor<String, VehicleState, Void, Void> {

    private final MongoTemplate mongoTemplate;
    private final String collectionName;

    public MongoSinkProcessor(MongoTemplate mongoTemplate, String collectionName) {
        this.mongoTemplate = mongoTemplate;
        this.collectionName = collectionName;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        // no-op
    }

    @Override
    public void process(Record<String, VehicleState> record) {
        VehicleState state = record.value();
        if (state == null || state.getDid() == null) {
            return;
        }

        try {
            VehicleDocument doc = VehicleDocument.fromState(state);

            Query query = new Query(Criteria.where("_id").is(doc.getDid()));

            Update update = new Update()
                    .set("did", doc.getDid())
                    .set("eventTime", doc.getEventTime())
                    .set("publishTime", doc.getPublishTime())
                    .set("ingestion_time", doc.getIngestionTime());

            // Only set signal fields that have been received at least once
            if (doc.getOdo() != null)      update.set("odo", doc.getOdo());
            if (doc.getSoc() != null)      update.set("soc", doc.getSoc());
            if (doc.getSpeed() != null)    update.set("speed", doc.getSpeed());
            if (doc.getIgnition() != null) update.set("ignition", doc.getIgnition());

            mongoTemplate.upsert(query, update, collectionName);

            log.info("did={} | Upserted to MongoDB | eventTime={} | odo={} soc={} speed={} ignition={}",
                    doc.getDid(), doc.getEventTime(),
                    doc.getOdo(), doc.getSoc(), doc.getSpeed(), doc.getIgnition());

        } catch (Exception e) {
            log.error("did={} | Failed to upsert to MongoDB", state.getDid(), e);
            // In production: push to a dead-letter topic or retry
        }
    }

    @Override
    public void close() {
        // no-op
    }
}

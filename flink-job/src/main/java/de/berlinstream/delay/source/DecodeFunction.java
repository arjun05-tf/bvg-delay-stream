package de.berlinstream.delay.source;

import de.berlinstream.delay.model.EnrichedEvent;
import de.berlinstream.delay.model.RawRecord;
import de.berlinstream.delay.proto.StopEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Decodes raw Kafka records into events, routing anything unreadable to a side
 * output that feeds the dead letter topic.
 *
 * <p>No record is ever silently dropped: a record either becomes an event or
 * becomes a dead letter, and both paths increment a counter.
 */
public class DecodeFunction extends ProcessFunction<RawRecord, EnrichedEvent> {

    private static final long serialVersionUID = 1L;

    private final OutputTag<RawRecord> deadLetterTag;

    private transient Counter decoded;
    private transient Counter failed;
    private transient Counter withoutDelay;
    private transient int lastSchemaId;

    /**
     * @param deadLetterTag side output tag for records that cannot be decoded
     */
    public DecodeFunction(OutputTag<RawRecord> deadLetterTag) {
        this.deadLetterTag = deadLetterTag;
    }

    @Override
    public void open(Configuration parameters) {
        decoded = getRuntimeContext().getMetricGroup().counter("recordsDecoded");
        failed = getRuntimeContext().getMetricGroup().counter("recordsUndecodable");
        withoutDelay = getRuntimeContext().getMetricGroup().counter("recordsWithoutDelay");
        lastSchemaId = -1;
        // Registered once. Registering a gauge per record would both leak and
        // trip Flink's duplicate metric name check.
        getRuntimeContext().getMetricGroup().gauge("writerSchemaId", () -> lastSchemaId);
    }

    @Override
    public void processElement(RawRecord record, Context context, Collector<EnrichedEvent> out) {
        ConfluentWireFormat.Decoded result;
        try {
            result = ConfluentWireFormat.decode(record.value);
        } catch (ConfluentWireFormat.DecodeException ex) {
            failed.inc();
            record.reason = ex.getMessage();
            context.output(deadLetterTag, record);
            return;
        }

        // Tracked, not enforced. A new writer schema id is the visible edge of
        // a producer rollout, and a backward compatible change needs no action
        // from this job: protobuf skips the fields it does not know.
        lastSchemaId = result.schemaId();

        StopEvent event = result.event();
        if (event.getObservedTime() <= 0) {
            failed.inc();
            record.reason = "observed_time is not set";
            context.output(deadLetterTag, record);
            return;
        }
        if (!event.getHasDelay()) {
            // A real and common case, not an error: the feed reported a time
            // for this stop but no delay against the schedule. Such an event
            // cannot contribute to a delay average, so it stops here and is
            // counted, which is what makes the coverage number honest.
            withoutDelay.inc();
            return;
        }

        EnrichedEvent enriched = new EnrichedEvent();
        enriched.tripId = event.getTripId();
        enriched.startDate = event.getStartDate();
        enriched.routeId = event.getRouteId();
        enriched.stopId = event.getStopId();
        enriched.stopSequence = event.getStopSequence();
        enriched.eventType = event.getEventTypeValue();
        enriched.observedTimeMillis = event.getObservedTime() * 1000L;
        enriched.delaySeconds = event.getDelaySeconds();
        enriched.hasDelay = true;
        enriched.scheduleRelationship = event.getScheduleRelationship();
        enriched.feedTimestampMillis = event.getFeedTimestamp() * 1000L;
        enriched.ingestTimestampMillis = event.getIngestTimestamp() * 1000L;
        decoded.inc();
        out.collect(enriched);
    }
}

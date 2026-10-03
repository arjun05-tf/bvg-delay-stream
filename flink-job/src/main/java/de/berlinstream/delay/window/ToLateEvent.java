package de.berlinstream.delay.window;

import de.berlinstream.delay.model.EnrichedEvent;
import de.berlinstream.delay.model.LateEvent;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Converts a late event into the row written to the late_events table.
 *
 * <p>Lateness is measured against the event's own window end, which is what
 * an operator wants to know: not that the record was late, but by how much,
 * so the allowed lateness setting can be argued from data.
 */
public class ToLateEvent implements MapFunction<EnrichedEvent, LateEvent> {

    private static final long serialVersionUID = 1L;

    private final long windowSizeMillis;

    /**
     * @param windowSizeMillis size of the tumbling window the late stream comes from
     */
    public ToLateEvent(long windowSizeMillis) {
        this.windowSizeMillis = windowSizeMillis;
    }

    @Override
    public LateEvent map(EnrichedEvent event) {
        long windowStart = Math.floorDiv(event.observedTimeMillis, windowSizeMillis) * windowSizeMillis;
        long windowEnd = windowStart + windowSizeMillis;

        LateEvent late = new LateEvent();
        late.tripId = event.tripId;
        late.startDate = event.startDate;
        late.stopId = event.stopId;
        late.stopSequence = event.stopSequence;
        late.eventType = event.eventType;
        late.routeId = event.routeId;
        late.observedTimeMillis = event.observedTimeMillis;
        late.delaySeconds = event.delaySeconds;
        late.hasDelay = event.hasDelay;
        late.feedTimestampMillis = event.feedTimestampMillis;
        // The feed timestamp is the moment the upstream system still believed
        // this event was worth restating, so it is the best available proxy
        // for when the record actually showed up.
        late.latenessSeconds = Math.max(0, (event.feedTimestampMillis - windowEnd) / 1000L);
        return late;
    }
}

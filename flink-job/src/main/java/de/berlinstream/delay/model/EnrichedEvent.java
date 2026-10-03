package de.berlinstream.delay.model;

/**
 * A stop event with its route and stop names resolved.
 *
 * <p>Carries only what the windows and the sink need. The protobuf message is
 * deliberately not passed downstream: Flink would serialise it through Kryo,
 * and a POJO keeps the state backend footprint small, which matters on a
 * laptop sized task manager.
 */
public class EnrichedEvent {

    public String tripId;
    public String startDate;
    public String routeId;
    public String stopId;
    public int stopSequence;
    public int eventType;
    public long observedTimeMillis;
    public int delaySeconds;
    public boolean hasDelay;
    public int scheduleRelationship;
    public long feedTimestampMillis;
    public long ingestTimestampMillis;

    public String routeShortName;
    public String stopName;

    public EnrichedEvent() {
    }

    /** Returns the window key: one series per route and stop. */
    public String aggregationKey() {
        return routeId + "|" + stopId;
    }

    @Override
    public String toString() {
        return "EnrichedEvent{" + routeShortName + "@" + stopName
                + " delay=" + (hasDelay ? delaySeconds + "s" : "unknown") + "}";
    }
}

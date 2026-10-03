package de.berlinstream.delay.model;

/**
 * Running aggregate for one window key.
 *
 * <p>Holds counts and sums only. Exact percentiles need a sketch and a merge
 * function; fixed buckets are exact, mergeable and cheap, and they answer the
 * question a transit dashboard asks, which is how often a line is late rather
 * than what the 99th percentile delay was to the second.
 */
public class DelayAccumulator {

    /** Delay at or below this is counted as early. */
    public static final int EARLY_THRESHOLD_SECONDS = -60;
    /** Delay at or below this, and above the early threshold, counts as on time. */
    public static final int ON_TIME_THRESHOLD_SECONDS = 120;
    /** Delay above the on-time threshold up to this counts as late. */
    public static final int LATE_THRESHOLD_SECONDS = 300;

    public String routeId = "";
    public String stopId = "";
    public String routeShortName = "";
    public String stopName = "";

    public long eventCount;
    public long sumDelaySeconds;
    public int minDelaySeconds = Integer.MAX_VALUE;
    public int maxDelaySeconds = Integer.MIN_VALUE;
    public long earlyCount;
    public long onTimeCount;
    public long lateCount;
    public long veryLateCount;
    public long maxIngestTimestampMillis;

    public DelayAccumulator() {
    }

    /** Adds one event to this accumulator. */
    public void add(EnrichedEvent event) {
        routeId = event.routeId;
        stopId = event.stopId;
        routeShortName = event.routeShortName;
        stopName = event.stopName;

        eventCount++;
        int delay = event.delaySeconds;
        sumDelaySeconds += delay;
        minDelaySeconds = Math.min(minDelaySeconds, delay);
        maxDelaySeconds = Math.max(maxDelaySeconds, delay);
        maxIngestTimestampMillis = Math.max(maxIngestTimestampMillis, event.ingestTimestampMillis);

        if (delay <= EARLY_THRESHOLD_SECONDS) {
            earlyCount++;
        } else if (delay <= ON_TIME_THRESHOLD_SECONDS) {
            onTimeCount++;
        } else if (delay <= LATE_THRESHOLD_SECONDS) {
            lateCount++;
        } else {
            veryLateCount++;
        }
    }

    /**
     * Merges another accumulator into this one.
     *
     * <p>Required by sliding windows, which Flink evaluates by merging pane
     * accumulators. The operation is associative and commutative, so the merge
     * order the runtime happens to choose cannot change the result.
     */
    public DelayAccumulator merge(DelayAccumulator other) {
        if (other.eventCount == 0) {
            return this;
        }
        if (eventCount == 0) {
            routeId = other.routeId;
            stopId = other.stopId;
            routeShortName = other.routeShortName;
            stopName = other.stopName;
        }
        eventCount += other.eventCount;
        sumDelaySeconds += other.sumDelaySeconds;
        minDelaySeconds = Math.min(minDelaySeconds, other.minDelaySeconds);
        maxDelaySeconds = Math.max(maxDelaySeconds, other.maxDelaySeconds);
        earlyCount += other.earlyCount;
        onTimeCount += other.onTimeCount;
        lateCount += other.lateCount;
        veryLateCount += other.veryLateCount;
        maxIngestTimestampMillis =
                Math.max(maxIngestTimestampMillis, other.maxIngestTimestampMillis);
        return this;
    }
}

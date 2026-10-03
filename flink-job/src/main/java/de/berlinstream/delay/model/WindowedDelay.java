package de.berlinstream.delay.model;

/** One aggregate row, ready to upsert. */
public class WindowedDelay {

    public long windowStartMillis;
    public long windowEndMillis;
    public String routeId;
    public String stopId;
    public String routeShortName;
    public String stopName;
    public long eventCount;
    public long sumDelaySeconds;
    public int minDelaySeconds;
    public int maxDelaySeconds;
    public long earlyCount;
    public long onTimeCount;
    public long lateCount;
    public long veryLateCount;
    public long maxIngestTimestampMillis;

    public WindowedDelay() {
    }

    @Override
    public String toString() {
        return "WindowedDelay{" + routeShortName + "@" + stopName
                + " [" + windowStartMillis + ", " + windowEndMillis + ") n=" + eventCount + "}";
    }
}

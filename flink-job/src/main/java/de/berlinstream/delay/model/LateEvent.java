package de.berlinstream.delay.model;

/**
 * An event that arrived after the watermark passed its window end.
 *
 * <p>Late records are not dropped. They are routed to a side output and
 * written to their own table, so the late rate is a measured number that
 * appears on the dashboard rather than an unverifiable claim in a README.
 */
public class LateEvent {

    public String tripId;
    public String startDate;
    public String stopId;
    public int stopSequence;
    public int eventType;
    public String routeId;
    public long observedTimeMillis;
    public int delaySeconds;
    public boolean hasDelay;
    public long feedTimestampMillis;
    public long latenessSeconds;

    public LateEvent() {
    }
}

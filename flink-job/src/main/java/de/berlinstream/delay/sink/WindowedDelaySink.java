package de.berlinstream.delay.sink;

import de.berlinstream.delay.config.JobConfig;
import de.berlinstream.delay.model.WindowedDelay;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Upserts window aggregates into one of the two aggregate tables.
 *
 * <p>The conflict target is the window identity: window start, route and stop.
 * Writing the same window twice therefore stores the same values rather than
 * adding a second row, which is the whole basis of the replay proof.
 */
public class WindowedDelaySink extends BatchedUpsertSink<WindowedDelay> {

    private static final long serialVersionUID = 1L;

    private final String table;
    private final String name;

    private transient long sampleCounter;
    private transient long lastLatencyMillis;
    private transient boolean latencyPending;

    /**
     * @param config job configuration
     * @param table target table, either delay_tumbling_1h or delay_sliding_1h_5m
     * @param name short metric name, for example "tumbling"
     */
    public WindowedDelaySink(JobConfig config, String table, String name) {
        super(config);
        this.table = table;
        this.name = name;
    }

    @Override
    protected String sinkName() {
        return name;
    }

    @Override
    protected String upsertSql() {
        return "INSERT INTO " + table + " ("
                + "window_start, window_end, route_id, stop_id, route_short_name, stop_name,"
                + "event_count, sum_delay_seconds, min_delay_seconds, max_delay_seconds,"
                + "early_count, on_time_count, late_count, very_late_count, updated_at) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?, now()) "
                + "ON CONFLICT (window_start, route_id, stop_id) DO UPDATE SET "
                + "window_end = EXCLUDED.window_end,"
                + "route_short_name = EXCLUDED.route_short_name,"
                + "stop_name = EXCLUDED.stop_name,"
                + "event_count = EXCLUDED.event_count,"
                + "sum_delay_seconds = EXCLUDED.sum_delay_seconds,"
                + "min_delay_seconds = EXCLUDED.min_delay_seconds,"
                + "max_delay_seconds = EXCLUDED.max_delay_seconds,"
                + "early_count = EXCLUDED.early_count,"
                + "on_time_count = EXCLUDED.on_time_count,"
                + "late_count = EXCLUDED.late_count,"
                + "very_late_count = EXCLUDED.very_late_count,"
                + "updated_at = now()";
    }

    @Override
    protected void bind(PreparedStatement statement, WindowedDelay row) throws SQLException {
        statement.setTimestamp(1, new Timestamp(row.windowStartMillis));
        statement.setTimestamp(2, new Timestamp(row.windowEndMillis));
        statement.setString(3, row.routeId);
        statement.setString(4, row.stopId);
        statement.setString(5, row.routeShortName);
        statement.setString(6, row.stopName);
        statement.setLong(7, row.eventCount);
        statement.setLong(8, row.sumDelaySeconds);
        statement.setInt(9, row.minDelaySeconds);
        statement.setInt(10, row.maxDelaySeconds);
        statement.setLong(11, row.earlyCount);
        statement.setLong(12, row.onTimeCount);
        statement.setLong(13, row.lateCount);
        statement.setLong(14, row.veryLateCount);

        sampleCounter++;
        if (sampleCounter % config().latencySampleEvery() == 0 && row.maxIngestTimestampMillis > 0) {
            lastLatencyMillis = System.currentTimeMillis() - row.maxIngestTimestampMillis;
            latencyPending = true;
        }
    }

    /**
     * Records one end-to-end latency sample inside the same transaction as the
     * batch it describes, so the measurement cannot survive a rolled back write.
     */
    @Override
    protected void afterFlush(Connection connection) throws SQLException {
        if (!latencyPending) {
            return;
        }
        try (PreparedStatement sample = connection.prepareStatement(
                "INSERT INTO e2e_latency_samples (sampled_at, latency_ms, stage) "
                        + "VALUES (now(), ?, ?)")) {
            sample.setLong(1, lastLatencyMillis);
            sample.setString(2, name);
            sample.executeUpdate();
        }
        latencyPending = false;
    }
}

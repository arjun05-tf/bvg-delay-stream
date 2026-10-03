package de.berlinstream.delay.sink;

import de.berlinstream.delay.config.JobConfig;
import de.berlinstream.delay.model.LateEvent;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

/** Upserts late records, keyed so a replayed late record overwrites itself. */
public class LateEventSink extends BatchedUpsertSink<LateEvent> {

    private static final long serialVersionUID = 1L;

    public LateEventSink(JobConfig config) {
        super(config);
    }

    @Override
    protected String sinkName() {
        return "lateEvents";
    }

    @Override
    protected String upsertSql() {
        return "INSERT INTO late_events ("
                + "trip_id, start_date, stop_id, stop_sequence, event_type, route_id,"
                + "observed_time, delay_seconds, feed_timestamp, lateness_seconds, recorded_at) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?, now()) "
                + "ON CONFLICT (observed_time, trip_id, start_date, stop_sequence, event_type) "
                + "DO UPDATE SET "
                + "delay_seconds = EXCLUDED.delay_seconds,"
                + "lateness_seconds = EXCLUDED.lateness_seconds,"
                + "feed_timestamp = EXCLUDED.feed_timestamp,"
                + "recorded_at = now()";
    }

    @Override
    protected void bind(PreparedStatement statement, LateEvent row) throws SQLException {
        statement.setString(1, row.tripId);
        statement.setString(2, row.startDate);
        statement.setString(3, row.stopId);
        statement.setInt(4, row.stopSequence);
        statement.setInt(5, row.eventType);
        statement.setString(6, row.routeId);
        statement.setTimestamp(7, new Timestamp(row.observedTimeMillis));
        if (row.hasDelay) {
            statement.setInt(8, row.delaySeconds);
        } else {
            statement.setNull(8, Types.INTEGER);
        }
        statement.setTimestamp(9, new Timestamp(row.feedTimestampMillis));
        statement.setLong(10, row.latenessSeconds);
    }
}

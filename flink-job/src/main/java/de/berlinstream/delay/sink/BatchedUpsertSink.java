package de.berlinstream.delay.sink;

import de.berlinstream.delay.config.JobConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batched, idempotent upsert sink.
 *
 * <p>Delivery semantics, which docs/recovery-semantics.md spells out in full:
 *
 * <ul>
 *   <li>Rows are buffered in a JDBC batch and flushed either when the batch
 *       fills or when a checkpoint barrier arrives. Flushing on the barrier is
 *       what makes a checkpoint mean "everything before this point is in the
 *       database".
 *   <li>After a restart from a checkpoint, Flink replays the records that
 *       followed that checkpoint. The sink will therefore write some rows a
 *       second time. Every statement is an upsert keyed on the window
 *       identity, so the second write stores the same values as the first.
 *       The result is effectively exactly once at the sink without a
 *       distributed transaction.
 *   <li>Deterministic input plus deterministic windowing plus idempotent
 *       upsert is also what makes a full replay from offset 0 reproduce the
 *       table byte for byte.
 * </ul>
 *
 * @param <T> row type written by this sink
 */
public abstract class BatchedUpsertSink<T> extends RichSinkFunction<T>
        implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(BatchedUpsertSink.class);

    private final JobConfig config;

    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient int pending;
    private transient Counter rowsWritten;

    protected BatchedUpsertSink(JobConfig config) {
        this.config = config;
    }

    /** Returns the upsert statement this sink prepares once per subtask. */
    protected abstract String upsertSql();

    /** Binds one row onto the prepared statement. */
    protected abstract void bind(PreparedStatement statement, T row) throws SQLException;

    /** Returns the metric group name for this sink. */
    protected abstract String sinkName();

    /** Called after a successful flush, so subclasses can emit side records. */
    protected void afterFlush(Connection connection) throws SQLException {
        // Default: nothing.
    }

    protected JobConfig config() {
        return config;
    }

    @Override
    public void open(Configuration parameters) throws SQLException {
        connection = openConnection();
        statement = connection.prepareStatement(upsertSql());
        pending = 0;
        rowsWritten = getRuntimeContext().getMetricGroup().counter(sinkName() + "RowsWritten");
    }

    private Connection openConnection() throws SQLException {
        Connection fresh =
                DriverManager.getConnection(config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword());
        fresh.setAutoCommit(false);
        return fresh;
    }

    @Override
    public void invoke(T row, Context context) throws SQLException {
        bind(statement, row);
        statement.addBatch();
        pending++;
        if (pending >= config.sinkBatchSize()) {
            flush();
        }
    }

    /**
     * Writes and commits the buffered batch.
     *
     * <p>The sink does not retry. A failed write fails the job, Flink restarts
     * it from the last checkpoint and the records since that checkpoint are
     * replayed into this same upsert, which converges on the same rows. A
     * retry loop inside the sink would duplicate that mechanism, hide the
     * failure from the checkpoint metrics and keep a broken job alive looking
     * healthy. The one thing the sink must do is leave no half written
     * transaction behind, which the rollback below guarantees.
     */
    protected void flush() throws SQLException {
        if (pending == 0) {
            return;
        }
        try {
            statement.executeBatch();
            afterFlush(connection);
            connection.commit();
            rowsWritten.inc(pending);
            pending = 0;
        } catch (SQLException ex) {
            LOG.error("sink flush failed for {} rows, rolling back", pending, ex);
            rollbackQuietly();
            pending = 0;
            throw ex;
        }
    }

    private void rollbackQuietly() {
        try {
            connection.rollback();
        } catch (SQLException ex) {
            LOG.warn("rollback failed", ex);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws SQLException {
        // Everything buffered must reach the database before this checkpoint
        // is acknowledged, otherwise a restart would skip it.
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // No operator state. The sink is stateless by design: correctness
        // comes from the idempotent upsert, not from remembering what it sent.
    }

    @Override
    public void close() throws SQLException {
        try {
            flush();
        } finally {
            closeQuietly();
        }
    }

    private void closeQuietly() {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException ex) {
            LOG.warn("failed to close statement", ex);
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ex) {
            LOG.warn("failed to close connection", ex);
        }
    }
}

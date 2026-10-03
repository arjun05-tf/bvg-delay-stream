package de.berlinstream.delay.window;

import de.berlinstream.delay.model.DelayAccumulator;
import de.berlinstream.delay.model.EnrichedEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental aggregation of delay events.
 *
 * <p>Incremental rather than a window function over the full element list: the
 * runtime keeps one small accumulator per window instead of every event, which
 * is what lets a sliding window with twelve open panes per key run inside a
 * 1.2 GB task manager.
 */
public class DelayAggregateFunction
        implements AggregateFunction<EnrichedEvent, DelayAccumulator, DelayAccumulator> {

    private static final long serialVersionUID = 1L;

    @Override
    public DelayAccumulator createAccumulator() {
        return new DelayAccumulator();
    }

    @Override
    public DelayAccumulator add(EnrichedEvent event, DelayAccumulator accumulator) {
        accumulator.add(event);
        return accumulator;
    }

    @Override
    public DelayAccumulator getResult(DelayAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public DelayAccumulator merge(DelayAccumulator left, DelayAccumulator right) {
        return left.merge(right);
    }
}

package de.berlinstream.delay;

import static org.junit.jupiter.api.Assertions.assertEquals;

import de.berlinstream.delay.model.DelayAccumulator;
import de.berlinstream.delay.model.EnrichedEvent;
import org.junit.jupiter.api.Test;

/** Bucket boundaries and merge behaviour of the aggregate. */
class DelayAccumulatorTest {

    private static EnrichedEvent event(int delaySeconds) {
        EnrichedEvent event = new EnrichedEvent();
        event.routeId = "17330_700";
        event.stopId = "de:11000:900100516::1";
        event.routeShortName = "M41";
        event.stopName = "Hermannplatz";
        event.delaySeconds = delaySeconds;
        event.hasDelay = true;
        event.ingestTimestampMillis = 1_790_977_030_000L;
        return event;
    }

    @Test
    void bucketsUseInclusiveUpperBounds() {
        DelayAccumulator accumulator = new DelayAccumulator();
        accumulator.add(event(-60));   // early, boundary
        accumulator.add(event(-59));   // on time, just above the boundary
        accumulator.add(event(120));   // on time, boundary
        accumulator.add(event(121));   // late
        accumulator.add(event(300));   // late, boundary
        accumulator.add(event(301));   // very late

        assertEquals(1, accumulator.earlyCount);
        assertEquals(2, accumulator.onTimeCount);
        assertEquals(2, accumulator.lateCount);
        assertEquals(1, accumulator.veryLateCount);
        assertEquals(6, accumulator.eventCount);
    }

    @Test
    void tracksSumMinAndMax() {
        DelayAccumulator accumulator = new DelayAccumulator();
        accumulator.add(event(30));
        accumulator.add(event(-120));
        accumulator.add(event(600));

        assertEquals(510, accumulator.sumDelaySeconds);
        assertEquals(-120, accumulator.minDelaySeconds);
        assertEquals(600, accumulator.maxDelaySeconds);
    }

    @Test
    void mergeIsCommutative() {
        DelayAccumulator left = new DelayAccumulator();
        left.add(event(10));
        left.add(event(400));
        DelayAccumulator right = new DelayAccumulator();
        right.add(event(-200));

        DelayAccumulator leftFirst = copyOf(left).merge(copyOf(right));
        DelayAccumulator rightFirst = copyOf(right).merge(copyOf(left));

        assertEquals(leftFirst.eventCount, rightFirst.eventCount);
        assertEquals(leftFirst.sumDelaySeconds, rightFirst.sumDelaySeconds);
        assertEquals(leftFirst.minDelaySeconds, rightFirst.minDelaySeconds);
        assertEquals(leftFirst.maxDelaySeconds, rightFirst.maxDelaySeconds);
        assertEquals(leftFirst.veryLateCount, rightFirst.veryLateCount);
    }

    @Test
    void mergingAnEmptyAccumulatorKeepsIdentity() {
        DelayAccumulator populated = new DelayAccumulator();
        populated.add(event(45));

        DelayAccumulator merged = copyOf(populated).merge(new DelayAccumulator());

        assertEquals(1, merged.eventCount);
        assertEquals("M41", merged.routeShortName);
        assertEquals(45, merged.sumDelaySeconds);
    }

    @Test
    void emptyMergedWithPopulatedTakesItsIdentity() {
        DelayAccumulator populated = new DelayAccumulator();
        populated.add(event(45));

        DelayAccumulator merged = new DelayAccumulator().merge(copyOf(populated));

        assertEquals(1, merged.eventCount);
        assertEquals("Hermannplatz", merged.stopName);
    }

    private static DelayAccumulator copyOf(DelayAccumulator source) {
        DelayAccumulator copy = new DelayAccumulator();
        copy.merge(source);
        return copy;
    }
}

package de.berlinstream.delay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import de.berlinstream.delay.proto.StopEvent;
import de.berlinstream.delay.source.ConfluentWireFormat;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/** Tests for the Confluent wire format header parser. */
class ConfluentWireFormatTest {

    private static StopEvent sampleEvent() {
        return StopEvent.newBuilder()
                .setTripId("307524656")
                .setStartDate("20261002")
                .setRouteId("17330_700")
                .setStopId("de:11000:900100516::1")
                .setStopSequence(0)
                .setEventType(StopEvent.EventType.EVENT_TYPE_ARRIVAL)
                .setObservedTime(1_790_976_422L)
                .setDelaySeconds(180)
                .setHasDelay(true)
                .setFeedTimestamp(1_790_977_022L)
                .setIngestTimestamp(1_790_977_030L)
                .build();
    }

    /** Builds a record the way the Confluent protobuf serialiser does. */
    private static byte[] encode(int schemaId, byte[] messageIndexes, StopEvent event)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0);
        out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
        out.write(messageIndexes);
        out.write(event.toByteArray());
        return out.toByteArray();
    }

    @Test
    void decodesSingleMessageShorthand() throws Exception {
        // A single zero varint means "the first message in the schema file".
        byte[] record = encode(42, new byte[] {0}, sampleEvent());

        ConfluentWireFormat.Decoded decoded = ConfluentWireFormat.decode(record);

        assertEquals(42, decoded.schemaId());
        assertEquals("307524656", decoded.event().getTripId());
        assertEquals(180, decoded.event().getDelaySeconds());
        assertTrue(decoded.event().getHasDelay());
    }

    @Test
    void decodesExplicitMessageIndexPath() throws Exception {
        // Count 1 followed by index 0, zig-zag encoded: [2, 0].
        byte[] record = encode(7, new byte[] {2, 0}, sampleEvent());

        ConfluentWireFormat.Decoded decoded = ConfluentWireFormat.decode(record);

        assertEquals(7, decoded.schemaId());
        assertEquals("de:11000:900100516::1", decoded.event().getStopId());
    }

    @Test
    void rejectsWrongMagicByte() throws Exception {
        byte[] record = encode(1, new byte[] {0}, sampleEvent());
        record[0] = 1;

        ConfluentWireFormat.DecodeException error =
                assertThrows(
                        ConfluentWireFormat.DecodeException.class,
                        () -> ConfluentWireFormat.decode(record));
        assertTrue(error.getMessage().contains("magic byte"));
    }

    @Test
    void rejectsTruncatedRecord() {
        assertThrows(
                ConfluentWireFormat.DecodeException.class,
                () -> ConfluentWireFormat.decode(new byte[] {0, 0, 0, 1}));
    }

    @Test
    void rejectsNullValue() {
        assertThrows(
                ConfluentWireFormat.DecodeException.class, () -> ConfluentWireFormat.decode(null));
    }

    @Test
    void unknownFieldsFromANewerWriterAreSkipped() throws Exception {
        // Simulate a producer that already writes field 99, which this
        // consumer's generated class does not know. Backward compatibility
        // means the record still decodes and the known fields are intact.
        byte[] payload = sampleEvent().toByteArray();
        ByteArrayOutputStream withUnknown = new ByteArrayOutputStream();
        withUnknown.write(payload);
        // Tag for field 99, wire type 0 (varint), then the value 7.
        withUnknown.write(new byte[] {(byte) 0x98, 0x06, 0x07});

        ByteArrayOutputStream record = new ByteArrayOutputStream();
        record.write(0);
        record.write(ByteBuffer.allocate(4).putInt(5).array());
        record.write(0);
        record.write(withUnknown.toByteArray());

        ConfluentWireFormat.Decoded decoded = ConfluentWireFormat.decode(record.toByteArray());

        assertEquals("307524656", decoded.event().getTripId());
        assertEquals(180, decoded.event().getDelaySeconds());
    }
}

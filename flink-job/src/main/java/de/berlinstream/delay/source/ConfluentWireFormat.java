package de.berlinstream.delay.source;

import com.google.protobuf.InvalidProtocolBufferException;
import de.berlinstream.delay.proto.StopEvent;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Decoder for the Confluent protobuf wire format.
 *
 * <p>A message produced by the Confluent protobuf serialiser is laid out as:
 *
 * <pre>
 *   byte 0        magic byte, always 0
 *   bytes 1..4    schema id, big endian int32
 *   varints       message index path, see below
 *   rest          the protobuf payload
 * </pre>
 *
 * <p>The message index path identifies which message inside the schema file
 * was serialised. A single zero varint is the shorthand for "the first message",
 * which is the common case and the one this producer always hits. The general
 * form is a count followed by that many indexes.
 *
 * <p>This class parses the header itself rather than pulling in the Confluent
 * deserialiser. The reason is deliberate: the job never needs to fetch a
 * schema to read a message. Protobuf is self describing enough that a consumer
 * compiled against an older copy of the schema skips unknown fields, which is
 * exactly the property the BACKWARD compatibility setting guarantees. The
 * schema id is still read and surfaced, so a job can count how many distinct
 * writer schemas it is seeing and an operator can watch a rollout happen.
 */
public final class ConfluentWireFormat implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final byte MAGIC_BYTE = 0x0;
    private static final int HEADER_BYTES = 5;

    private ConfluentWireFormat() {
    }

    /** A decoded message together with the schema id its writer used. */
    public record Decoded(StopEvent event, int schemaId) {
    }

    /**
     * Decodes one Kafka record value.
     *
     * @param value raw record value, including the Confluent header
     * @return the decoded event and the writer schema id
     * @throws DecodeException if the header is absent or malformed, or the
     *     payload is not a StopEvent
     */
    public static Decoded decode(byte[] value) throws DecodeException {
        if (value == null || value.length < HEADER_BYTES + 1) {
            throw new DecodeException("record is shorter than the Confluent header");
        }
        if (value[0] != MAGIC_BYTE) {
            throw new DecodeException(
                    "unexpected magic byte 0x" + Integer.toHexString(value[0] & 0xff)
                            + ", value was not written by the schema registry serialiser");
        }

        ByteBuffer buffer = ByteBuffer.wrap(value);
        buffer.position(1);
        int schemaId = buffer.getInt();

        int indexCount = readVarint(buffer);
        if (indexCount < 0) {
            throw new DecodeException("negative message index count " + indexCount);
        }
        // Zero is the shorthand for the single index [0]; any other count is
        // followed by that many index varints.
        for (int i = 0; i < indexCount; i++) {
            readVarint(buffer);
        }

        byte[] payload = new byte[buffer.remaining()];
        buffer.get(payload);
        try {
            return new Decoded(StopEvent.parseFrom(payload), schemaId);
        } catch (InvalidProtocolBufferException ex) {
            throw new DecodeException("payload is not a StopEvent: " + ex.getMessage(), ex);
        }
    }

    /**
     * Reads one zig-zag encoded varint, which is what the Confluent format uses
     * for message indexes.
     */
    private static int readVarint(ByteBuffer buffer) throws DecodeException {
        int result = 0;
        int shift = 0;
        while (shift < 32) {
            if (!buffer.hasRemaining()) {
                throw new DecodeException("truncated message index varint");
            }
            byte current = buffer.get();
            result |= (current & 0x7f) << shift;
            if ((current & 0x80) == 0) {
                return (result >>> 1) ^ -(result & 1);
            }
            shift += 7;
        }
        throw new DecodeException("message index varint is longer than 32 bits");
    }

    /** Raised when a record cannot be decoded. Carries no payload by design. */
    public static final class DecodeException extends Exception {
        private static final long serialVersionUID = 1L;

        public DecodeException(String message) {
            super(message);
        }

        public DecodeException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

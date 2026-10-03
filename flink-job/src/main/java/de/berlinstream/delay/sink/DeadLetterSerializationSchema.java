package de.berlinstream.delay.sink;

import de.berlinstream.delay.model.RawRecord;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * Serialises a dead letter record.
 *
 * <p>The original bytes are written back unchanged, so a fixed consumer can
 * reprocess the topic later. The reason and the source coordinates travel as
 * headers, which lets an operator filter the topic without decoding bodies.
 */
public class DeadLetterSerializationSchema implements KafkaRecordSerializationSchema<RawRecord> {

    private static final long serialVersionUID = 1L;

    private final String topic;

    public DeadLetterSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            RawRecord record, KafkaSinkContext context, Long timestamp) {
        ProducerRecord<byte[], byte[]> out = new ProducerRecord<>(topic, null, record.value);
        out.headers()
                .add(new RecordHeader("reason", bytes(record.reason)))
                .add(new RecordHeader("source_topic", bytes(record.topic)))
                .add(new RecordHeader("source_partition", bytes(String.valueOf(record.partition))))
                .add(new RecordHeader("source_offset", bytes(String.valueOf(record.offset))));
        return out;
    }

    private static byte[] bytes(String value) {
        return (value == null ? "unknown" : value).getBytes(StandardCharsets.UTF_8);
    }
}

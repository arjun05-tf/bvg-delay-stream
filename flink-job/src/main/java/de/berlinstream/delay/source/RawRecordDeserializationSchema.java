package de.berlinstream.delay.source;

import de.berlinstream.delay.model.RawRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Hands every Kafka record downstream as raw bytes plus its coordinates.
 *
 * <p>Decoding happens in a process function rather than here, because a
 * deserialisation schema that throws takes the whole job down with it, and a
 * single poisoned message must not be able to do that. The coordinates travel
 * with the bytes so a dead letter record names the exact offset to inspect.
 */
public class RawRecordDeserializationSchema implements KafkaRecordDeserializationSchema<RawRecord> {

    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawRecord> out) {
        out.collect(
                new RawRecord(
                        record.value(),
                        null,
                        record.topic(),
                        record.partition(),
                        record.offset()));
    }

    @Override
    public TypeInformation<RawRecord> getProducedType() {
        return TypeInformation.of(RawRecord.class);
    }
}

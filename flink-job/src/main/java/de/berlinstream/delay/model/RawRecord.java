package de.berlinstream.delay.model;

import java.util.Objects;

/**
 * A Kafka record that could not be decoded, on its way to the dead letter topic.
 *
 * <p>Plain POJO with a no-argument constructor and public fields, so Flink uses
 * its own POJO serialiser instead of falling back to Kryo.
 */
public class RawRecord {

    public byte[] value;
    public String reason;
    public String topic;
    public int partition;
    public long offset;

    public RawRecord() {
    }

    public RawRecord(byte[] value, String reason, String topic, int partition, long offset) {
        this.value = value;
        this.reason = reason;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "RawRecord{" + topic + "-" + partition + "@" + offset + " reason=" + reason + "}";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof RawRecord that)) {
            return false;
        }
        return partition == that.partition && offset == that.offset
                && Objects.equals(topic, that.topic) && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, offset, reason);
    }
}

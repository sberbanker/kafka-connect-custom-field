package ru.rgs.kafka.connect.transforms;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import static org.junit.Assert.*;

public class LowerCaseTopicTest {
    @Test
    public void test() {
        final SinkRecord input = new SinkRecord(
                "TeSt",
                1,
                null,
                "",
                null,
                "",
                1234123L,
                12341312L,
                TimestampType.NO_TIMESTAMP_TYPE
        );
        try (LowerCaseTopic<SinkRecord> transform = new LowerCaseTopic<>()) {
            final SinkRecord actual = transform.apply(input);
            assertEquals("Topic should match.", "test", actual.topic());
        }
    }
}

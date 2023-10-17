package ru.rgs.kafka.connect.transforms;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.ReplaceField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LowerCaseFieldTest {
    private final LowerCaseField<SinkRecord> xform = new LowerCaseField.Value<>();

    @AfterEach
    public void teardown() {
        xform.close();
    }

    @Test
    public void tombstoneSchemaless() {
        final Map<String, String> props = new HashMap<>();
        xform.configure(props);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
        assertNull(transformedRecord.valueSchema());
    }

    @Test
    public void tombstoneWithSchema() {
        final Map<String, String> props = new HashMap<>();
        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("Dont", Schema.STRING_SCHEMA)
                .field("aBc", Schema.INT32_SCHEMA)
                .field("fo0", Schema.BOOLEAN_SCHEMA)
                .field("ETC", Schema.STRING_SCHEMA)
                .build();

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
        assertEquals(schema, transformedRecord.valueSchema());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void schemaless() {
        final Map<String, String> props = new HashMap<>();
        xform.configure(props);

        final Map<String, Object> key = new HashMap<>();
        key.put("DonT", "whatever");

        final Map<String, Object> value = new HashMap<>();
        value.put("Dont", "whatever");
        value.put("aBc", 42);
        value.put("fo0", true);
        value.put("ETC", "etc");

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Map<String, Object> updatedValue = (Map<String, Object>) transformedRecord.value();
        assertEquals("whatever", updatedValue.get("dont"));
        assertEquals(42, updatedValue.get("abc"));
        assertEquals(true, updatedValue.get("fo0"));
        assertEquals("etc", updatedValue.get("etc"));
    }

    @Test
    public void withSchema() {
        final Map<String, String> props = new HashMap<>();
        xform.configure(props);

        final Schema keySchema = SchemaBuilder.struct()
                .field("DonT", Schema.STRING_SCHEMA)
                .build();

        final Struct key = new Struct(keySchema);
        key.put("DonT", "whatever");

        final Schema schema = SchemaBuilder.struct()
                .field("DonT", Schema.STRING_SCHEMA)
                .field("aBc", Schema.INT32_SCHEMA)
                .field("Fo0", Schema.BOOLEAN_SCHEMA)
                .field("ETC", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("DonT", "whatever");
        value.put("aBc", 42);
        value.put("Fo0", true);
        value.put("ETC", "etc");

        final SinkRecord record = new SinkRecord("test", 0, keySchema, key, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals(String.valueOf("whatever"), updatedValue.getString("dont"));
        assertEquals(Integer.valueOf(42), updatedValue.getInt32("abc"));
        assertEquals(true, updatedValue.getBoolean("fo0"));
        assertEquals(String.valueOf("etc"), updatedValue.getString("etc"));
    }

    @Test
    public void testReplaceFieldVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), xform.version());
    }
}

/*
 * Copyright © 2023 sberbanker (faleksei@mail.ru)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.rgs.kafka.connect.transforms;

import io.debezium.data.VariableScaleDecimal;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.Console;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ClearUnicodeNullTest {
    private final ClearUnicodeNull<SourceRecord> xformKey = new ClearUnicodeNull.Key<>();
    private final ClearUnicodeNull<SourceRecord> xformValue = new ClearUnicodeNull.Value<>();

    @AfterEach
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test
    public void testVersionRetrievedFromAppInfoParser() {

        assertEquals(AppInfoParser.getVersion(), xformKey.version());
        assertEquals(AppInfoParser.getVersion(), xformValue.version());

        assertEquals(xformKey.version(), xformValue.version());
    }

    @Test
    public void testConfigNullFields() {
        assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(ClearUnicodeNull.FIELDS_CONFIG, null)));
    }

    @Test
    public void testConfigEmptyFields() {
        assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(ClearUnicodeNull.FIELDS_CONFIG, "")));
    }

    @Test
    public void testNullKeyRecordSchemaless() {
        xformKey.configure(Collections.singletonMap(ClearUnicodeNull.FIELDS_CONFIG, "foo"));
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                null, null, Schema.STRING_SCHEMA, "value");
        SourceRecord transformed = xformKey.apply(original);
        assertEquals(original, transformed);
    }

    @Test
    public void testNullValueRecordSchemaless() {
        xformValue.configure(Collections.singletonMap(ClearUnicodeNull.FIELDS_CONFIG, "foo"));
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", null, null);
        SourceRecord transformed = xformValue.apply(original);
        assertEquals(original, transformed);
    }

    private SourceRecord createRecordWithSchema(Schema keySchema, Object keyValue, Schema valueSchema, Object valueValue) {
        return new SourceRecord(null, null, "topic", 0, keySchema, keyValue, valueSchema, valueValue);
    }

    private SourceRecord createRecordSchemaless(Object key, Object value) {
        return new SourceRecord(null, null, "topic", 0, null, key, null, value);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testKeySchemaless() {
        xformKey.configure(Collections.singletonMap(ClearUnicodeNull.FIELDS_CONFIG, "bar"));

        Map<String, Object> key = new HashMap<>(2);
        key.put("foo", "A");
        key.put("bar", "B\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000");

        SourceRecord original = createRecordSchemaless(key, "value");
        SourceRecord transformed = xformKey.apply(original);

        assertEquals("B", ((HashMap<String, Object>)transformed.key()).get("bar"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testValueSchemaless() {
        xformValue.configure(Collections.singletonMap(ClearUnicodeNull.FIELDS_CONFIG, "bar"));

        Map<String, Object> value = new HashMap<>(2);
        value.put("foo", "A");
        value.put("bar", "B\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000");

        SourceRecord original = createRecordSchemaless(null, value);
        SourceRecord transformed = xformValue.apply(original);

        assertEquals("B", ((HashMap<String, Object>)transformed.value()).get("bar"));
    }

    @Test
    public void testKeyWithSchema() {
        xformKey.configure(Collections.singletonMap(ClearUnicodeNull.FIELDS_CONFIG, "bar"));

        String foo_value = "A";
        String bar_value = "B\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000";

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("foo", Schema.STRING_SCHEMA);
        builder.field("bar", Schema.STRING_SCHEMA);

        Schema supportedTypesSchema = builder.build();

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("foo", foo_value);
        recordValue.put("bar", bar_value);

        SourceRecord original = createRecordWithSchema(supportedTypesSchema, recordValue, null, null);
        SourceRecord transformed = xformKey.apply(original);

        assertEquals("B", ((Struct) transformed.key()).get("bar"));
    }

    @Test
    public void testValueWithSchema() {
        xformValue.configure(Collections.singletonMap(ClearUnicodeNull.FIELDS_CONFIG, "bar"));

        String foo_value = "A";
        String bar_value = "B\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000";

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("foo", Schema.STRING_SCHEMA);
        builder.field("bar", Schema.STRING_SCHEMA);

        Schema supportedTypesSchema = builder.build();

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("foo", foo_value);
        recordValue.put("bar", bar_value);

        SourceRecord original = createRecordWithSchema(null, null, supportedTypesSchema, recordValue);
        SourceRecord transformed = xformValue.apply(original);

        assertEquals("B", ((Struct) transformed.value()).get("bar"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNonStringSchemaless() {
        xformKey.configure(Collections.singletonMap(ClearUnicodeNull.FIELDS_CONFIG, "bar"));

        Map<String, Object> key = new HashMap<>(2);
        key.put("foo", "A");
        key.put("bar", 2L);

        SourceRecord original = createRecordSchemaless(key, "value");
        SourceRecord transformed = xformKey.apply(original);

        assertEquals(2L, ((HashMap<String, Object>)transformed.key()).get("bar"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAbsentFieldSchemaless() {
        xformKey.configure(Collections.singletonMap(ClearUnicodeNull.FIELDS_CONFIG, "baz"));

        Map<String, Object> key = new HashMap<>(2);
        key.put("foo", "B\\u0000");
        key.put("bar", 2L);

        SourceRecord original = createRecordSchemaless(key, "value");
        SourceRecord transformed = xformKey.apply(original);

        assertEquals("B\\u0000", ((HashMap<String, Object>)transformed.key()).get("foo"));
        assertEquals(2L, ((HashMap<String, Object>)transformed.key()).get("bar"));
    }

    @Test
    public void testNonStringSchema() {
        xformKey.configure(Collections.singletonMap(ClearUnicodeNull.FIELDS_CONFIG, "bar"));

        String foo_value = "A";
        Long bar_value = 2L;

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("foo", Schema.STRING_SCHEMA);
        builder.field("bar", Schema.INT64_SCHEMA);

        Schema supportedTypesSchema = builder.build();

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("foo", foo_value);
        recordValue.put("bar", bar_value);

        SourceRecord original = createRecordWithSchema(supportedTypesSchema, recordValue, null, null);
        SourceRecord transformed = xformKey.apply(original);

        assertEquals("A", ((Struct) transformed.key()).get("foo"));
        assertEquals(2L, ((Struct) transformed.key()).get("bar"));
    }

    @Test
    public void testAbsentFieldSchema() {
        xformKey.configure(Collections.singletonMap(ClearUnicodeNull.FIELDS_CONFIG, "baz"));

        String foo_value = "B\\u0000";
        Long bar_value = 2L;

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("foo", Schema.STRING_SCHEMA);
        builder.field("bar", Schema.INT64_SCHEMA);

        Schema supportedTypesSchema = builder.build();

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("foo", foo_value);
        recordValue.put("bar", bar_value);

        SourceRecord original = createRecordWithSchema(supportedTypesSchema, recordValue, null, null);
        SourceRecord transformed = xformKey.apply(original);

        assertEquals("B\\u0000", ((Struct) transformed.key()).get("foo"));
        assertEquals(2L, ((Struct) transformed.key()).get("bar"));
    }
}

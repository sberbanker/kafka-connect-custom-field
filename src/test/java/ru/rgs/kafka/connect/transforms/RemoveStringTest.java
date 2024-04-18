/*
 * Copyright © 2024 sberbanker (faleksei@mail.ru)
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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class RemoveStringTest {
    private final RemoveString<SourceRecord> xformKey = new RemoveString.Key<>();
    private final RemoveString<SourceRecord> xformValue = new RemoveString.Value<>();

    private static final Schema SCHEMA = SchemaBuilder.struct()
            .field("foo", Schema.STRING_SCHEMA)
            .field("bar", Schema.STRING_SCHEMA)
            .field("baz", Schema.FLOAT64_SCHEMA)
            .build();
    private static final Map<String, Object> VALUES = new HashMap<>();
    private static final Struct VALUES_WITH_SCHEMA = new Struct(SCHEMA);

    static {
        VALUES.put("foo", "TESTME");
        VALUES.put("bar", "\\u0000\\u0000TESTME\\u0000\\u0000\\u0000\\u0000");
        VALUES.put("baz", 12.34d);

        VALUES_WITH_SCHEMA.put("foo", "TESTME");
        VALUES_WITH_SCHEMA.put("bar", "\\u0000\\u0000TESTME\\u0000\\u0000\\u0000\\u0000");
        VALUES_WITH_SCHEMA.put("baz", 12.34d);
    }

    private SourceRecord createRecordSchemaless(Object key, Object value) {
        return new SourceRecord(null, null, "topic", 0, null, key, null, value);
    }

    private SourceRecord createRecordWithSchema(Schema keySchema, Object keyValue, Schema valueSchema, Object valueValue) {
        return new SourceRecord(null, null, "topic", 0, keySchema, keyValue, valueSchema, valueValue);
    }

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

    private static Map<String, String> config (String fields, String removement) {
        Map<String, String> config = new HashMap<>();
        config.put(RemoveString.FIELDS_CONFIG, fields);
        config.put(RemoveString.REMOVEMENT_CONFIG, removement);
        return config;
    }

    @Test
    public void testConfigNullFields() {
        assertThrows(ConfigException.class, () -> xformKey.configure(config(null, "\\u0000")));
        assertThrows(ConfigException.class, () -> xformKey.configure(config("foo", null)));
    }

    @Test
    public void testConfigEmptyFields() {
        assertThrows(ConfigException.class, () -> xformKey.configure(config("", "\\u0000")));
        assertThrows(ConfigException.class, () -> xformKey.configure(config("foo", "")));
    }

    @Test
    public void testNullKeySchemaless() {
        xformKey.configure(config("bar", "\\u0000"));
        SourceRecord original = createRecordSchemaless(null, VALUES);
        SourceRecord transformed = xformKey.apply(original);
        assertEquals(original, transformed);
    }

    @Test
    public void testNullValueSchemaless() {
        xformKey.configure(config("bar", "\\u0000"));
        SourceRecord original = createRecordSchemaless(VALUES, null);
        SourceRecord transformed = xformValue.apply(original);
        assertEquals(original, transformed);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testKeySchemaless() {
        xformKey.configure(config("bar", "\\u0000"));

        SourceRecord original = createRecordSchemaless(VALUES, VALUES);
        SourceRecord transformed = xformKey.apply(original);

        assertEquals("TESTME", ((HashMap<String, Object>)transformed.key()).get("bar"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testValueSchemaless() {
        xformValue.configure(config("bar", "\\u0000"));

        SourceRecord original = createRecordSchemaless(VALUES, VALUES);
        SourceRecord transformed = xformValue.apply(original);

        assertEquals("TESTME", ((HashMap<String, Object>)transformed.value()).get("bar"));
    }

    @Test
    public void testKeyWithSchema() {
        xformKey.configure(config("bar", "\\u0000"));

        SourceRecord original = createRecordWithSchema(SCHEMA, VALUES_WITH_SCHEMA, SCHEMA, VALUES_WITH_SCHEMA);
        SourceRecord transformed = xformKey.apply(original);

        assertEquals("TESTME", ((Struct) transformed.key()).get("bar"));
    }

    @Test
    public void testValueWithSchema() {
        xformValue.configure(config("bar", "\\u0000"));

        SourceRecord original = createRecordWithSchema(SCHEMA, VALUES_WITH_SCHEMA, SCHEMA, VALUES_WITH_SCHEMA);
        SourceRecord transformed = xformValue.apply(original);

        assertEquals("TESTME", ((Struct) transformed.value()).get("bar"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNonStringSchemaless() {
        xformValue.configure(config("baz", "\\u0000"));

        SourceRecord original = createRecordSchemaless(null, VALUES);
        SourceRecord transformed = xformValue.apply(original);

        assertEquals(VALUES.get("baz"), ((HashMap<String, Object>)transformed.value()).get("baz"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAbsentFieldSchemaless() {
        xformValue.configure(config("qux", "\\u0000"));

        SourceRecord original = createRecordSchemaless(null, VALUES);
        SourceRecord transformed = xformValue.apply(original);

        assertEquals(VALUES.get("foo"), ((HashMap<String, Object>)transformed.value()).get("foo"));
        assertEquals(VALUES.get("bar"), ((HashMap<String, Object>)transformed.value()).get("bar"));
        assertEquals(VALUES.get("baz"), ((HashMap<String, Object>)transformed.value()).get("baz"));
    }

    @Test
    public void testNonStringSchema() {
        xformValue.configure(config("baz", "\\u0000"));

        SourceRecord original = createRecordWithSchema(SCHEMA, VALUES_WITH_SCHEMA, SCHEMA, VALUES_WITH_SCHEMA);
        SourceRecord transformed = xformValue.apply(original);

        assertEquals(VALUES.get("baz"), ((Struct) transformed.value()).get("baz"));
    }

    @Test
    public void testAbsentFieldSchema() {
        xformValue.configure(config("qux", "\\u0000"));

        SourceRecord original = createRecordWithSchema(SCHEMA, VALUES_WITH_SCHEMA, SCHEMA, VALUES_WITH_SCHEMA);
        SourceRecord transformed = xformValue.apply(original);

        assertEquals(VALUES.get("foo"), ((Struct) transformed.value()).get("foo"));
        assertEquals(VALUES.get("bar"), ((Struct) transformed.value()).get("bar"));
        assertEquals(VALUES.get("baz"), ((Struct) transformed.value()).get("baz"));
    }
}

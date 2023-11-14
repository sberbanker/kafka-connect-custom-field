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

public class StructConverterTest {
    private final StructConverter<SourceRecord> xformKey = new StructConverter.Key<>();
    private final StructConverter<SourceRecord> xformValue = new StructConverter.Value<>();

    @AfterEach
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test
    public void testCastVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), xformKey.version());
        assertEquals(AppInfoParser.getVersion(), xformValue.version());

        assertEquals(xformKey.version(), xformValue.version());
    }

    @Test
    public void testConfigInvalidSchemaType() {
        assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(StructConverter.FIELDS_CONFIG, "foo:faketype")));
    }

    @Test
    public void testConfigInvalidTargetType() {
        assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(StructConverter.FIELDS_CONFIG, "foo:array")));
    }

    @Test
    public void testUnsupportedTargetType() {
        assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(StructConverter.FIELDS_CONFIG, "foo:bytes")));
    }

    @Test
    public void testConfigInvalidMap() {
        assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(StructConverter.FIELDS_CONFIG, "foo:int8:extra")));
    }

    @Test
    public void testConfigMixWholeAndFieldTransformation() {
        assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(StructConverter.FIELDS_CONFIG, "foo:int8,int32")));
    }

    @Test
    public void castNullKeyRecordSchemaless() {
        xformKey.configure(Collections.singletonMap(StructConverter.FIELDS_CONFIG, "foo:float64"));
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                null, null, Schema.STRING_SCHEMA, "value");
        SourceRecord transformed = xformKey.apply(original);
        assertEquals(original, transformed);
    }

    @Test
    public void castNullValueRecordSchemaless() {
        xformValue.configure(Collections.singletonMap(StructConverter.FIELDS_CONFIG, "foo:float64"));
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", null, null);
        SourceRecord transformed = xformValue.apply(original);
        assertEquals(original, transformed);
    }

    @Test
    public void castWithSchemaToFloat() {
        xformValue.configure(Collections.singletonMap(StructConverter.FIELDS_CONFIG, "foo:float64,bar:float64,baz:float64"));

        Struct foo_value = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("19821"));
        Struct bar_value = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("20220512000207051551"));
        Struct baz_value = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("2022051200.02070515505"));

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("foo", VariableScaleDecimal.schema());
        builder.field("bar", VariableScaleDecimal.schema());
        builder.field("baz", VariableScaleDecimal.schema());

        Schema supportedTypesSchema = builder.build();

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("foo", foo_value);
        recordValue.put("bar", bar_value);
        recordValue.put("baz", baz_value);

        SourceRecord transformed = xformValue.apply(
                new SourceRecord(null, null, "topic", 0,supportedTypesSchema, recordValue));

        assertEquals(19821D, ((Struct) transformed.value()).get("foo"));
        assertEquals(20220512000207051551D, ((Struct) transformed.value()).get("bar"));
        assertEquals(2022051200.02070515505D, ((Struct) transformed.value()).get("baz"));
    }

    @Test
    public void castWithSchemaToString() {
        xformValue.configure(Collections.singletonMap(StructConverter.FIELDS_CONFIG, "foo:string,bar:string,baz:string"));

        Struct foo_value = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("19821"));
        Struct bar_value = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("20220512000207051551"));
        Struct baz_value = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("2022051200.02070515505"));

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("foo", VariableScaleDecimal.schema());
        builder.field("bar", VariableScaleDecimal.schema());
        builder.field("baz", VariableScaleDecimal.schema());

        Schema supportedTypesSchema = builder.build();

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("foo", foo_value);
        recordValue.put("bar", bar_value);
        recordValue.put("baz", baz_value);

        SourceRecord transformed = xformValue.apply(
                new SourceRecord(null, null, "topic", 0, supportedTypesSchema, recordValue));

        assertEquals("19821", ((Struct) transformed.value()).get("foo"));
        assertEquals("20220512000207051551", ((Struct) transformed.value()).get("bar"));
        assertEquals("2022051200.02070515505", ((Struct) transformed.value()).get("baz"));
    }

    @Test
    public void castWithSchemaAll() {
        final Map<String, String> props = new HashMap<>();
        xformValue.configure(props);

        Struct foo_value = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("19821"));
        Struct bar_value = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("20220512000207051551"));
        Struct baz_value = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("2022051200.02070515505"));

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("foo", VariableScaleDecimal.schema());
        builder.field("bar", VariableScaleDecimal.schema());
        builder.field("baz", VariableScaleDecimal.schema());
        builder.field("null", VariableScaleDecimal.optionalSchema());
        builder.field("int8", Schema.INT8_SCHEMA);
        builder.field("decimal", Decimal.schema(new BigDecimal(1982).scale()));

        Schema supportedTypesSchema = builder.build();

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("foo", foo_value);
        recordValue.put("bar", bar_value);
        recordValue.put("baz", baz_value);
        recordValue.put("int8", (byte) 8);
        recordValue.put("decimal", new BigDecimal(1982));

        SourceRecord transformed = xformValue.apply(
                new SourceRecord(null, null, "topic", 0, supportedTypesSchema, recordValue));

        assertEquals(19821D, ((Struct) transformed.value()).get("foo"));
        assertEquals(20220512000207051551D, ((Struct) transformed.value()).get("bar"));
        assertEquals(2022051200.02070515505D, ((Struct) transformed.value()).get("baz"));
        assertNull(((Struct) transformed.value()).get("null"));
        assertEquals((byte) 8, ((Struct) transformed.value()).get("int8"));
        assertEquals(new BigDecimal(1982), ((Struct) transformed.value()).get("decimal"));
    }
}
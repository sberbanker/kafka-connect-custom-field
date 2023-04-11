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

package ru.rgs.kafka.connect.transforms.predicates;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.junit.jupiter.api.Test;

import java.util.*;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;

public class HasHeaderKeyValueTest {

    @Test
    public void testNameRequiredInConfig() {
        Map<String, String> props = new HashMap<>();
        ConfigException e = assertThrows(ConfigException.class, () -> config(props));
        assertTrue(e.getMessage().contains("Missing required configuration \"header_name\""));
    }

    @Test
    public void testNameMayNotBeEmptyInConfig() {
        Map<String, String> props = new HashMap<>();
        props.put("header_name", "");
        ConfigException e = assertThrows(ConfigException.class, () -> config(props));
        assertTrue(e.getMessage().contains("String must be non-empty"));
    }

    @Test
    public void testValueRequiredInConfig() {
        Map<String, String> props = new HashMap<>();
        props.put("header_name", "foo");
        ConfigException e = assertThrows(ConfigException.class, () -> config(props));
        assertTrue(e.getMessage().contains("Missing required configuration \"header_value\""));
    }

    @Test
    public void testValueMayNotBeEmptyInConfig() {
        Map<String, String> props = new HashMap<>();
        props.put("header_name", "foo");
        props.put("header_value", "");
        ConfigException e = assertThrows(ConfigException.class, () -> config(props));
        assertTrue(e.getMessage().contains("String must be non-empty"));
    }

    @Test
    public void testConfig() {
        try (HasHeaderKeyValue<SourceRecord> predicate = new HasHeaderKeyValue<>();) {
            Map<String, String> props = new HashMap<>();
            props.put("header_name", "foo");
            props.put("header_value", "bar");
            predicate.config().validate(props);

            List<ConfigValue> configs = new ArrayList<ConfigValue>();

            props.replace("header_name", "");
            configs = predicate.config().validate(props);
            assertEquals(singletonList("Invalid value  for configuration header_name: String must be non-empty"), Objects.requireNonNull(GetConfigValue("header_name", configs)).errorMessages());

            props.replace("header_name", "foo");
            props.replace("header_value", "");
            configs = predicate.config().validate(props);
            assertEquals(singletonList("Invalid value  for configuration header_value: String must be non-empty"), Objects.requireNonNull(GetConfigValue("header_value", configs)).errorMessages());
        }
    }

    private ConfigValue GetConfigValue (String name, List<ConfigValue> configValues) {
        for (ConfigValue configValue : configValues) {
            if (configValue.name().equals(name)) {
                return configValue;
            }
        }
        return null;
    }

    @Test
    public void testTest() {
        try (HasHeaderKeyValue<SourceRecord> predicate = new HasHeaderKeyValue<>();) {
            Map<String, String> props = new HashMap<>();
            props.put("header_name", "foo");
            props.put("header_value", "bar");
            predicate.configure(props);

            assertTrue(predicate.test(recordWithHeaders(new String[][]{{"foo","bar"}})));
            assertTrue(predicate.test(recordWithHeaders(new String[][]{{"foo","bar"},{"foo","baz"}})));
            assertTrue(predicate.test(recordWithHeaders(new String[][]{{"foo","baz"},{"foo","bar"}})));
            assertFalse(predicate.test(recordWithHeaders(new String[][]{{"foo","baz"}})));
            assertFalse(predicate.test(recordWithHeaders(new String[][]{{"foo","baz"},{"foo","baz"}})));
            assertFalse(predicate.test(recordWithHeaders(new String[][]{})));
            assertFalse(predicate.test(new SourceRecord(null, null, null, null, null)));
        }
    }

    private SimpleConfig config(Map<String, String> props) {
        try (HasHeaderKeyValue<SourceRecord> predicate = new HasHeaderKeyValue<>();) {
            return new SimpleConfig(predicate.config(), props);
        }
    }

    private SourceRecord recordWithHeaders(String[][] headersArray) {
        SourceRecord rec = new SourceRecord(null,null,null,null,null);

        for (String[] header : headersArray) {
            rec.headers().add(new TestHeader(header[0], new SchemaAndValue(null, header[1])));
        }

        return rec;
    }

    private static class TestHeader implements Header {

        private final String key;
        private final SchemaAndValue schemaAndValue;

        public TestHeader(String key, SchemaAndValue schemaAndValue) {
            this.key = key;
            this.schemaAndValue = schemaAndValue;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Schema schema() { return null; }

        @Override
        public Object value() {
            return this.schemaAndValue.value();
        }

        @Override
        public Header with(Schema schema, Object value) {
            return null;
        }

        @Override
        public Header rename(String key) {
            return null;
        }
    }
}
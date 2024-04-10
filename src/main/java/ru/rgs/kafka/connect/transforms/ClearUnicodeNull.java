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

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ClearUnicodeNull<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    public static final String OVERVIEW_DOC = "This transformation is used to remove Unicode null symbol 0x00 from string values.";

    public static final String FIELDS_CONFIG = "fields";
    private static final String FIELD_DEFAULT = "";
    private static final String PURPOSE = "remove Unicode null symbol";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.STRING, FIELD_DEFAULT,
//                    new ConfigDef.Validator() {
//                        @SuppressWarnings("unchecked")
//                        @Override
//                        public void ensureValid(String name, Object valueObject) {
//                            String value = (String) valueObject;
//                            if (value == null || value.isEmpty()) {
//                                throw new ConfigException("Must specify at least one field.");
//                            }
//                        }
//
//                        @Override
//                        public String toString() {
//                            return "list of comma-delimited fields, e.g. <code>foo,bar,abc,xyz</code>";
//                        }
//                    },
                    ConfigDef.Importance.HIGH,
                    "The field containing null unicode symbol \\u0000");

    Set<String> fields = Collections.emptySet();
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void configure(Map<String, ?> configs) {

        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        final String fieldsConfig = config.getString(FIELDS_CONFIG);

        if (fieldsConfig == null || fieldsConfig.isEmpty()) {
            throw new ConfigException("Must specify at least one field.");
        }

        fields = Arrays.stream(fieldsConfig.split(",")).map(String::trim).collect(Collectors.toSet());
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }
    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {

        Object rawValue = operatingValue(record);
        if (rawValue == null) {
            return record;
        } else {
            final Map<String, Object> value = requireMap(rawValue, PURPOSE);

            // convert each field:
            for (String field : fields) {
                if (!value.containsKey(field)) continue;
                if (!value.get(field).getClass().getName().equals("java.lang.String")) continue;

                value.put(field, value.get(field).toString().replace("\\u0000", ""));
            }
            return newRecord(record, null, value);
        }
    }

    private R applyWithSchema(R record) {

        if (operatingValue(record) == null) {
            return record;
        }

        final Schema schema = operatingSchema(record);
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        // convert each field:
        for (Field field : value.schema().fields()) {
            String fieldName = field.name();
            if (fields.contains(fieldName) && value.get(fieldName).getClass().getName().equals("java.lang.String") ) {
                value.put(field, value.getString(field.name()).replace("\\u0000", ""));
            }
        }

//        for (String field : fields) {
//
//            if (!value.get(field).getClass().getName().equals("java.lang.String")) continue;
//            value.put(field, value.getString(field).replace("\\u0000", ""));
//        }
        return newRecord(record, schema, value);
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ClearUnicodeNull<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends ClearUnicodeNull<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }

}
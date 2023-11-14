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

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.data.*;
import io.debezium.data.VariableScaleDecimal;

import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.EnumSet;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class StructConverter<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
    private static final Logger log = LoggerFactory.getLogger(StructConverter.class);
    protected abstract Schema operatingSchema(R record);
    protected abstract Object operatingValue(R record);
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static final String OVERVIEW_DOC =
            "Cast fields of the entire key or value from Struct type of Debezium to a specific type."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
                    + "or value (<code>" + Value.class.getName() + "</code>).";

    public static final String STRUCT_TYPE_CONFIG = "struct_type";
    public static final String STRUCT_TYPE_DEFAULT = "VariableScaleDecimal";
    public static final String FIELDS_CONFIG = "fields";
    private static final String FIELDS_DEFAULT = "all:float64";
    private static final String PURPOSE = "cast struct types";

    private static final Set<Schema.Type> SUPPORTED_CAST_INPUT_TYPES = EnumSet.of(
            Schema.Type.STRUCT
    );
    private static final Set<Schema.Type> SUPPORTED_CAST_OUTPUT_TYPES = EnumSet.of(
            Schema.Type.FLOAT64, Schema.Type.STRING
    );

    private Map<String, Schema.Type> casts;
    private Cache<Schema, Schema> schemaUpdateCache;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, FIELDS_DEFAULT, new ConfigDef.Validator() {
                        @SuppressWarnings("unchecked")
                        @Override
                        public void ensureValid(String name, Object valueObject) {
                            List<String> value = (List<String>) valueObject;
                            if (value == null || value.isEmpty()) {
                                throw new ConfigException("Must specify at least one field to cast.");
                            }
                            parseFieldTypes(value);
                        }

                        @Override
                        public String toString() {
                            return "list of colon-delimited pairs, e.g. <code>foo:bar,abc:xyz</code>";
                        }
                    },
                    ConfigDef.Importance.HIGH,
                    "List of fields and the type to cast them to of the form field1:type,field2:type. "
                            + "Valid types are int8, int16, int32, int64, float32, float64, boolean, and string.")
            .define(STRUCT_TYPE_CONFIG, ConfigDef.Type.STRING, STRUCT_TYPE_DEFAULT, ConfigDef.Importance.HIGH,
                    "Debezium struct type name.");

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

//    private static final String WHOLE_VALUE_CAST = null;
    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        casts = parseFieldTypes(config.getList(FIELDS_CONFIG));
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        }

        if (operatingSchema(record) == null) {
            return record;
//            return applySchemaless(record);
        }
        else {
//            return record;
            return applyWithSchema(record);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    private R applyWithSchema(R record) {
        Schema valueSchema = operatingSchema(record);
        Schema updatedSchema = getOrBuildSchema(valueSchema);

        // Casting within a struct
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            if (field.schema().name() == null || (casts.get(field.name()) == null && !field.schema().name().equals(VariableScaleDecimal.LOGICAL_NAME))) {
                updatedValue.put(updatedSchema.field(field.name()), value.get(field));
                continue;
            }

            final Struct origFieldValue = (Struct) value.get(field);
            final Schema.Type targetType = casts.get(field.name()) != null ? casts.get(field.name()) : casts.get("all");
            final Object newFieldValue = targetType != null ? castValueToType(field.schema(), origFieldValue, targetType) : origFieldValue;
            log.trace("Cast field '{}' from '{}' to '{}'", field.name(), origFieldValue, newFieldValue);
            updatedValue.put(updatedSchema.field(field.name()), newFieldValue);
        }
        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema getOrBuildSchema(Schema valueSchema) {
        Schema updatedSchema = schemaUpdateCache.get(valueSchema);
        if (updatedSchema != null)
            return updatedSchema;

        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(valueSchema, SchemaBuilder.struct());
        for (Field field : valueSchema.fields()) {
            if (field.schema().name() == null) {
                builder.field(field.name(), field.schema());
            } else if ((casts.containsKey(field.name()) || casts.containsKey("all")) && field.schema().name().equals(VariableScaleDecimal.LOGICAL_NAME)) {
                log.trace("Cast schema for field {}", field.name());
                var schema = casts.get(field.name()) !=null ? casts.get(field.name()) : casts.get("all");
                SchemaBuilder fieldBuilder = convertFieldType(schema);

                if (field.schema().isOptional())
                    fieldBuilder.optional();
                if (field.schema().defaultValue() != null)
                    fieldBuilder.defaultValue(field.schema().defaultValue());

                builder.field(field.name(), fieldBuilder.build());
            } else {
                builder.field(field.name(), field.schema());
            }
        }

        if (valueSchema.isOptional())
            builder.optional();
        if (valueSchema.defaultValue() != null)
            builder.defaultValue(valueSchema.defaultValue());

        updatedSchema = builder.build();
        schemaUpdateCache.put(valueSchema, updatedSchema);
        return updatedSchema;
    }

    private SchemaBuilder convertFieldType(Schema.Type type) {
        switch (type) {
            case FLOAT64:
                return SchemaBuilder.float64();
            case STRING:
                return SchemaBuilder.string();
            default:
                throw new DataException("Unexpected type in Cast transformation: " + type);
        }
    }

    private static Object castValueToType(Schema schema, Struct value, Schema.Type targetType) {
        try {
            if (value == null) return null;

            Schema.Type inferredType = schema == null ? ConnectSchema.schemaType(value.getClass()) : schema.type();
            if (inferredType == null) {
                throw new DataException("Cast transformation was passed a value of type " + value.getClass()
                        + " which is not supported by Connect's data API");
            }

            switch (targetType) {
                case FLOAT64:
                    return VariableScaleDecimal.toLogical(value).toDouble();
                case STRING:
                    return VariableScaleDecimal.toLogical(value).toString();
                default:
                    throw new DataException(targetType + " is not supported in the Cast transformation.");
            }
        } catch (NumberFormatException e) {
            throw new DataException("Value (" + value.toString() + ") was out of range for requested data type", e);
        }
    }

    private static Map<String, Schema.Type> parseFieldTypes(List<String> mappings) {
        final Map<String, Schema.Type> m = new HashMap<>();
        for (String mapping : mappings) {
            final String[] parts = mapping.split(":");
            if (parts.length != 2) {
                throw new ConfigException(StructConverter.FIELDS_CONFIG, mappings, "Invalid fields mapping: " + mapping);
            } else {
                Schema.Type type;
                try {
                    type = Schema.Type.valueOf(parts[1].trim().toUpperCase(Locale.ROOT));
                } catch (IllegalArgumentException e) {
                    throw new ConfigException("Invalid type found in casting spec: " + parts[1].trim(), e);
                }
                m.put(parts[0].trim(), validCastType(type));
            }
        }
        return m;
    }

    private static Schema.Type validCastType(Schema.Type type) {
        if (!SUPPORTED_CAST_OUTPUT_TYPES.contains(type)) {
            throw new ConfigException("Cast transformation does not support casting to " +
                    type + "; supported types are " + SUPPORTED_CAST_OUTPUT_TYPES);
        }
        return type;
    }

    public static final class Key<R extends ConnectRecord<R>> extends StructConverter<R> {
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

    public static final class Value<R extends ConnectRecord<R>> extends StructConverter<R> {
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

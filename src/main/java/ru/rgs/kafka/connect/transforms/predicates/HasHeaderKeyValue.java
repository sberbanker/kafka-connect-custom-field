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

import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.transforms.predicates.Predicate;

/**
 * A predicate which is true for records with at least one header with the configured name and a given value.
 * @param <R> The type of connect record.
 */
public class HasHeaderKeyValue<R extends ConnectRecord<R>> implements Predicate<R> {

    private static final String NAME_CONFIG = "header_name";
    private static final String VALUE_CONFIG = "header_value";
    public static final String OVERVIEW_DOC = "A predicate which is true for records with at least one header with the configured name and a given value.";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                    "The header name.")
            .define(VALUE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                    "The header value.");
    private String header_name;
    private String header_value;


    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public boolean test(R record) {
        Iterator<Header> headerIterator = record.headers().allWithName(header_name);
        if (!headerIterator.hasNext()) return false;
        while(headerIterator.hasNext()){
            if (headerIterator.next().value() == header_value) return true;
        }
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        header_name = config.getString(NAME_CONFIG);
        header_value = config.getString(VALUE_CONFIG);
    }

    @Override
    public String toString() {
        return "HasHeaderKeyValue{" +
                "header_name='" + header_name + '\'' + ',' +
                "header_value='" + header_value + '\'' +
                '}';
    }
}
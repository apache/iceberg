/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.transforms;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class CopyValue<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final String SOURCE_FIELD = "source.field";
  private static final String TARGET_FIELD = "target.field";

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              SOURCE_FIELD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Source field name.")
          .define(
              TARGET_FIELD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Target field name.");

  private String sourceField;
  private String targetField;
  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public void configure(Map<String, ?> props) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    sourceField = config.getString(SOURCE_FIELD);
    targetField = config.getString(TARGET_FIELD);
    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
  }

  @Override
  public R apply(R record) {
    if (record.value() == null) {
      return record;
    } else if (record.valueSchema() == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applySchemaless(R record) {
    Map<String, Object> value = Requirements.requireMap(record.value(), "copy value");

    Map<String, Object> updatedValue = Maps.newHashMap(value);
    updatedValue.put(targetField, value.get(sourceField));

    return newRecord(record, null, updatedValue);
  }

  private R applyWithSchema(R record) {
    Struct value = Requirements.requireStruct(record.value(), "copy value");

    Schema updatedSchema = schemaUpdateCache.get(value.schema());
    if (updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(value.schema());
      schemaUpdateCache.put(value.schema(), updatedSchema);
    }

    Struct updatedValue = new Struct(updatedSchema);

    for (Field field : value.schema().fields()) {
      updatedValue.put(field.name(), value.get(field));
    }
    updatedValue.put(targetField, value.get(sourceField));

    return newRecord(record, updatedSchema, updatedValue);
  }

  private Schema makeUpdatedSchema(Schema schema) {
    SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field : schema.fields()) {
      builder.field(field.name(), field.schema());
    }
    builder.field(targetField, schema.field(sourceField).schema());

    return builder.build();
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        updatedSchema,
        updatedValue,
        record.timestamp());
  }
}

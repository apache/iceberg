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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;

public class EnvelopeTransform<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final String COL_OBJ = "obj";
  private static final String COL_LOAD_TS = "load_ts";
  private static final String COL_EVENT_TS = "event_ts";

  public static final ConfigDef CONFIG_DEF = new ConfigDef();

  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public void configure(Map<String, ?> props) {
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

  @SuppressWarnings("JavaUtilDate")
  private R applySchemaless(R record) {
    Map<String, Object> value = Requirements.requireMap(record.value(), "envelope transform");

    Map<String, Object> envelope = Maps.newHashMap();
    envelope.put(COL_OBJ, value);
    envelope.put(COL_LOAD_TS, System.currentTimeMillis());
    envelope.put(
        COL_EVENT_TS, record.timestamp() != null ? new java.util.Date(record.timestamp()) : null);

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        null,
        envelope,
        record.timestamp());
  }

  @SuppressWarnings("JavaUtilDate")
  private R applyWithSchema(R record) {
    Struct value = Requirements.requireStruct(record.value(), "envelope transform");

    Schema envelopeSchema = schemaUpdateCache.get(value.schema());
    if (envelopeSchema == null) {
      envelopeSchema = makeEnvelopeSchema(value.schema());
      schemaUpdateCache.put(value.schema(), envelopeSchema);
    }

    Struct envelope = new Struct(envelopeSchema);
    envelope.put(COL_OBJ, value);
    envelope.put(COL_LOAD_TS, new java.util.Date(System.currentTimeMillis()));
    envelope.put(
        COL_EVENT_TS, record.timestamp() != null ? new java.util.Date(record.timestamp()) : null);

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        envelopeSchema,
        envelope,
        record.timestamp());
  }

  private Schema makeEnvelopeSchema(Schema valueSchema) {
    return SchemaBuilder.struct()
        .field(COL_OBJ, valueSchema)
        .field(COL_LOAD_TS, Timestamp.SCHEMA)
        .field(COL_EVENT_TS, Timestamp.builder().optional().build())
        .build();
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }
}

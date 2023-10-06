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
package io.tabular.iceberg.connect.transforms;

import java.util.Map;
import jdk.jfr.Experimental;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
public class DebeziumTransform<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger LOG = LoggerFactory.getLogger(DebeziumTransform.class.getName());
  private static final ConfigDef EMPTY_CONFIG = new ConfigDef();

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

  private R applyWithSchema(R record) {
    Struct value = Requirements.requireStruct(record.value(), "Debezium transform");

    String op = mapOperation(value.get("op").toString());

    Struct payload;
    Schema payloadSchema;
    if (op.equals(CdcConstants.OP_DELETE)) {
      payload = value.getStruct("before");
      payloadSchema = value.schema().field("before").schema();
    } else {
      payload = value.getStruct("after");
      payloadSchema = value.schema().field("after").schema();
    }

    Schema newValueSchema = makeUpdatedSchema(payloadSchema);
    Struct newValue = new Struct(newValueSchema);

    for (Field field : payloadSchema.fields()) {
      newValue.put(field.name(), payload.get(field));
    }
    newValue.put(CdcConstants.COL_CDC_OP, op);
    newValue.put(CdcConstants.COL_CDC_TS, new java.util.Date(value.getInt64("ts_ms")));
    newValue.put(CdcConstants.COL_CDC_TABLE, tableNameFromSourceStruct(value.getStruct("source")));

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        newValueSchema,
        newValue,
        record.timestamp());
  }

  @SuppressWarnings("unchecked")
  private R applySchemaless(R record) {
    Map<String, Object> value = Requirements.requireMap(record.value(), "Debezium transform");

    String op = mapOperation(value.get("op").toString());

    Object payload;
    if (op.equals(CdcConstants.OP_DELETE)) {
      payload = value.get("before");
    } else {
      payload = value.get("after");
    }

    if (!(payload instanceof Map)) {
      LOG.debug("Unable to transform Debezium record, payload is not a map, skipping");
      return null;
    }

    Map<String, Object> newValue = Maps.newHashMap((Map<String, Object>) payload);
    newValue.put(CdcConstants.COL_CDC_OP, op);
    newValue.put(CdcConstants.COL_CDC_TS, value.get("ts_ms"));
    newValue.put(CdcConstants.COL_CDC_TABLE, tableNameFromSourceMap(value.get("source")));

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        null,
        newValue,
        record.timestamp());
  }

  private String mapOperation(String originalOp) {
    switch (originalOp) {
      case "u":
        return CdcConstants.OP_UPDATE;
      case "d":
        return CdcConstants.OP_DELETE;
      default:
        // Debezium ops "c", "r", and any others
        return CdcConstants.OP_INSERT;
    }
  }

  private String tableNameFromSourceStruct(Struct source) {
    String db;
    if (source.schema().field("schema") != null) {
      // prefer schema if present, e.g. for Postgres
      db = source.getString("schema");
    } else {
      db = source.getString("db");
    }
    String table = source.getString("table");
    return db + "." + table;
  }

  private String tableNameFromSourceMap(Object source) {
    Map<String, Object> map = Requirements.requireMap(source, "Debezium transform");

    String db;
    if (map.containsKey("schema")) {
      // prefer schema if present, e.g. for Postgres
      db = map.get("schema").toString();
    } else {
      db = map.get("db").toString();
    }
    String table = map.get("table").toString();
    return db + "." + table;
  }

  private Schema makeUpdatedSchema(Schema schema) {
    SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field : schema.fields()) {
      builder.field(field.name(), field.schema());
    }
    builder.field(CdcConstants.COL_CDC_OP, Schema.STRING_SCHEMA);
    builder.field(CdcConstants.COL_CDC_TS, Timestamp.SCHEMA);
    builder.field(CdcConstants.COL_CDC_TABLE, Schema.STRING_SCHEMA);

    return builder.build();
  }

  @Override
  public ConfigDef config() {
    return EMPTY_CONFIG;
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {}
}

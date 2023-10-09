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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumTransform<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger LOG = LoggerFactory.getLogger(DebeziumTransform.class.getName());

  private static final String CDC_TARGET_PATTERN = "cdc.target.pattern";
  private static final String DB_PLACEHOLDER = "{db}";
  private static final String TABLE_PLACEHOLDER = "{table}";

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              CDC_TARGET_PATTERN,
              ConfigDef.Type.STRING,
              null,
              Importance.MEDIUM,
              "Pattern to use for setting the CDC target field value.");

  private String cdcTargetPattern;

  @Override
  public void configure(Map<String, ?> props) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    cdcTargetPattern = config.getString(CDC_TARGET_PATTERN);
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

    Schema newValueSchema = makeUpdatedSchema(payloadSchema, record.keySchema());
    Struct newValue = new Struct(newValueSchema);

    for (Field field : payloadSchema.fields()) {
      newValue.put(field.name(), payload.get(field));
    }

    newValue.put(CdcConstants.COL_CDC_OP, op);
    newValue.put(CdcConstants.COL_CDC_TS, new java.util.Date(value.getInt64("ts_ms")));
    setTableAndTargetFromSourceStruct(value.getStruct("source"), newValue);

    if (record.keySchema() != null) {
      newValue.put(CdcConstants.COL_CDC_KEY, record.key());
    }

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
    setTableAndTargetFromSourceMap(value.get("source"), newValue);

    if (record.key() instanceof Map) {
      newValue.put(CdcConstants.COL_CDC_KEY, record.key());
    }

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

  private void setTableAndTargetFromSourceStruct(Struct source, Struct value) {
    String db;
    if (source.schema().field("schema") != null) {
      // prefer schema if present, e.g. for Postgres
      db = source.getString("schema");
    } else {
      db = source.getString("db");
    }
    String table = source.getString("table");

    value.put(CdcConstants.COL_CDC_TABLE, db + "." + table);
    value.put(CdcConstants.COL_CDC_TARGET, target(db, table));
  }

  private void setTableAndTargetFromSourceMap(Object source, Map<String, Object> value) {
    Map<String, Object> map = Requirements.requireMap(source, "Debezium transform");

    String db;
    if (map.containsKey("schema")) {
      // prefer schema if present, e.g. for Postgres
      db = map.get("schema").toString();
    } else {
      db = map.get("db").toString();
    }
    String table = map.get("table").toString();

    value.put(CdcConstants.COL_CDC_TABLE, db + "." + table);
    value.put(CdcConstants.COL_CDC_TARGET, target(db, table));
  }

  private String target(String db, String table) {
    return cdcTargetPattern == null || cdcTargetPattern.isEmpty()
        ? db + "." + table
        : cdcTargetPattern.replace(DB_PLACEHOLDER, db).replace(TABLE_PLACEHOLDER, table);
  }

  private Schema makeUpdatedSchema(Schema schema, Schema keySchema) {
    SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field : schema.fields()) {
      builder.field(field.name(), field.schema());
    }

    builder
        .field(CdcConstants.COL_CDC_OP, Schema.STRING_SCHEMA)
        .field(CdcConstants.COL_CDC_TS, Timestamp.SCHEMA)
        .field(CdcConstants.COL_CDC_TABLE, Schema.STRING_SCHEMA)
        .field(CdcConstants.COL_CDC_TARGET, Schema.STRING_SCHEMA);

    if (keySchema != null) {
      builder.field(CdcConstants.COL_CDC_KEY, keySchema);
    }

    return builder.build();
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {}
}

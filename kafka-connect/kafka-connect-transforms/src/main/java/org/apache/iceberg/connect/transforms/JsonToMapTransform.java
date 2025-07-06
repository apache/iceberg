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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class JsonToMapTransform<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String JSON_LEVEL = "json.root";

  private static final ObjectReader MAPPER = new ObjectMapper().reader();

  private boolean startAtRoot = false;

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              JSON_LEVEL,
              ConfigDef.Type.BOOLEAN,
              false,
              ConfigDef.Importance.MEDIUM,
              "Boolean value to start at root. False is one level in from the root");

  private static final String ALL_JSON_SCHEMA_FIELD = "payload";
  private static final Schema JSON_MAP_SCHEMA =
      SchemaBuilder.struct()
          .field(
              ALL_JSON_SCHEMA_FIELD,
              SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build())
          .build();

  @Override
  public R apply(R record) {
    if (record.value() == null) {
      return record;
    } else {
      return process(record);
    }
  }

  private R process(R record) {
    if (!(record.value() instanceof String)) {
      throw new JsonToMapException("record value is not a string, use StringConverter");
    }

    String json = (String) record.value();
    JsonNode obj;

    try {
      obj = MAPPER.readTree(json);
    } catch (Exception e) {
      throw new JsonToMapException(
          String.format(
              "record.value is not valid json for record.value: %s", collectRecordDetails(record)),
          e);
    }

    if (!(obj instanceof ObjectNode)) {
      throw new JsonToMapException(
          String.format(
              "Expected json object for record.value after parsing: %s",
              collectRecordDetails(record)));
    }

    if (startAtRoot) {
      return singleField(record, (ObjectNode) obj);
    }
    return structRecord(record, (ObjectNode) obj);
  }

  private R singleField(R record, ObjectNode obj) {
    Struct struct =
        new Struct(JSON_MAP_SCHEMA)
            .put(ALL_JSON_SCHEMA_FIELD, JsonToMapUtils.populateMap(obj, Maps.newHashMap()));
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        JSON_MAP_SCHEMA,
        struct,
        record.timestamp(),
        record.headers());
  }

  private R structRecord(R record, ObjectNode contents) {
    SchemaBuilder builder = SchemaBuilder.struct();
    contents.fields().forEachRemaining(entry -> JsonToMapUtils.addField(entry, builder));
    Schema schema = builder.build();
    Struct value = JsonToMapUtils.addToStruct(contents, schema, new Struct(schema));
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        schema,
        value,
        record.timestamp(),
        record.headers());
  }

  private String collectRecordDetails(R record) {
    if (record instanceof SinkRecord) {
      SinkRecord sinkRecord = (SinkRecord) record;
      return String.format(
          "topic %s partition %s offset %s",
          sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
    } else {
      return String.format(
          Locale.ROOT, "topic %s partition %S", record.topic(), record.kafkaPartition());
    }
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    startAtRoot = config.getBoolean(JSON_LEVEL);
  }
}

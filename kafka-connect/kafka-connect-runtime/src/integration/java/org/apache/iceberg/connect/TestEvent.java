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
package org.apache.iceberg.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Date;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

public class TestEvent {

  public static final Schema TEST_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "type", Types.StringType.get()),
              Types.NestedField.required(3, "ts", Types.TimestampType.withZone()),
              Types.NestedField.required(4, "payload", Types.StringType.get())),
          ImmutableSet.of(1));

  public static final org.apache.kafka.connect.data.Schema TEST_CONNECT_SCHEMA =
      SchemaBuilder.struct()
          .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .field("type", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("ts", Timestamp.SCHEMA)
          .field("payload", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("op", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA);

  public static final PartitionSpec TEST_SPEC =
      PartitionSpec.builderFor(TEST_SCHEMA).day("ts").build();

  private static final JsonConverter JSON_CONVERTER = new JsonConverter();

  static {
    JSON_CONVERTER.configure(
        ImmutableMap.of(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName()));
  }

  private final long id;
  private final String type;
  private final Instant ts;
  private final String payload;
  private final String op;

  public TestEvent(long id, String type, Instant ts, String payload) {
    this(id, type, ts, payload, null);
  }

  public TestEvent(long id, String type, Instant ts, String payload, String op) {
    this.id = id;
    this.type = type;
    this.ts = ts;
    this.payload = payload;
    this.op = op;
  }

  public long id() {
    return id;
  }

  protected String serialize(boolean useSchema) {
    try {
      Struct value =
          new Struct(TEST_CONNECT_SCHEMA)
              .put("id", id)
              .put("type", type)
              .put("ts", Date.from(ts))
              .put("payload", payload)
              .put("op", op);

      String convertMethod =
          useSchema ? "convertToJsonWithEnvelope" : "convertToJsonWithoutEnvelope";
      JsonNode json =
          DynMethods.builder(convertMethod)
              .hiddenImpl(
                  JsonConverter.class, org.apache.kafka.connect.data.Schema.class, Object.class)
              .build(JSON_CONVERTER)
              .invoke(TestEvent.TEST_CONNECT_SCHEMA, value);
      return TestContext.MAPPER.writeValueAsString(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}

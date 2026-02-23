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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Date;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

@SuppressWarnings("JavaUtilDate")
public class TestEnvelopeTransform {

  @Test
  public void testNullPassthrough() {
    try (EnvelopeTransform<SinkRecord> smt = new EnvelopeTransform<>()) {
      smt.configure(Collections.emptyMap());
      SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);
      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isNull();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSchemaless() {
    try (EnvelopeTransform<SinkRecord> smt = new EnvelopeTransform<>()) {
      smt.configure(Collections.emptyMap());

      Map<String, Object> value = Maps.newHashMap();
      value.put("id", 123L);
      value.put("data", "foobar");

      long kafkaTs = 1700000000000L;
      SinkRecord record = new SinkRecord("topic", 0, null, null, null, value, 0, kafkaTs, null);
      SinkRecord result = smt.apply(record);

      Map<String, Object> envelope = (Map<String, Object>) result.value();
      assertThat(envelope).containsKey("obj");
      assertThat(envelope).containsKey("load_ts");
      assertThat(envelope).containsKey("event_ts");

      Map<String, Object> obj = (Map<String, Object>) envelope.get("obj");
      assertThat(obj.get("id")).isEqualTo(123L);
      assertThat(obj.get("data")).isEqualTo("foobar");

      assertThat(envelope.get("event_ts")).isEqualTo(new Date(kafkaTs));
      assertThat(result.valueSchema()).isNull();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSchemalessNullTimestamp() {
    try (EnvelopeTransform<SinkRecord> smt = new EnvelopeTransform<>()) {
      smt.configure(Collections.emptyMap());

      Map<String, Object> value = Maps.newHashMap();
      value.put("id", 1L);

      SinkRecord record = new SinkRecord("topic", 0, null, null, null, value, 0);
      SinkRecord result = smt.apply(record);

      Map<String, Object> envelope = (Map<String, Object>) result.value();
      assertThat(envelope.get("event_ts")).isNull();
    }
  }

  @Test
  public void testWithSchema() {
    try (EnvelopeTransform<SinkRecord> smt = new EnvelopeTransform<>()) {
      smt.configure(Collections.emptyMap());

      Schema schema =
          SchemaBuilder.struct()
              .field("id", Schema.INT64_SCHEMA)
              .field("data", Schema.STRING_SCHEMA)
              .build();

      Struct value = new Struct(schema).put("id", 123L).put("data", "foobar");

      long kafkaTs = 1700000000000L;
      SinkRecord record = new SinkRecord("topic", 0, null, null, schema, value, 0, kafkaTs, null);
      SinkRecord result = smt.apply(record);

      Schema envelopeSchema = result.valueSchema();
      assertThat(envelopeSchema.field("obj").schema()).isEqualTo(schema);
      assertThat(envelopeSchema.field("load_ts").schema()).isEqualTo(Timestamp.SCHEMA);
      assertThat(envelopeSchema.field("event_ts").schema().isOptional()).isTrue();

      Struct envelope = (Struct) result.value();
      Struct obj = envelope.getStruct("obj");
      assertThat(obj.getInt64("id")).isEqualTo(123L);
      assertThat(obj.getString("data")).isEqualTo("foobar");
      assertThat(envelope.get("event_ts")).isEqualTo(new Date(kafkaTs));
    }
  }

  @Test
  public void testWithSchemaNullTimestamp() {
    try (EnvelopeTransform<SinkRecord> smt = new EnvelopeTransform<>()) {
      smt.configure(Collections.emptyMap());

      Schema schema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();
      Struct value = new Struct(schema).put("id", 1L);

      SinkRecord record = new SinkRecord("topic", 0, null, null, schema, value, 0);
      SinkRecord result = smt.apply(record);

      Struct envelope = (Struct) result.value();
      assertThat(envelope.get("event_ts")).isNull();
    }
  }

  @Test
  public void testSchemaCaching() {
    try (EnvelopeTransform<SinkRecord> smt = new EnvelopeTransform<>()) {
      smt.configure(Collections.emptyMap());

      Schema schema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();
      Struct value1 = new Struct(schema).put("id", 1L);
      Struct value2 = new Struct(schema).put("id", 2L);

      SinkRecord record1 = new SinkRecord("topic", 0, null, null, schema, value1, 0);
      SinkRecord record2 = new SinkRecord("topic", 0, null, null, schema, value2, 1);

      SinkRecord result1 = smt.apply(record1);
      SinkRecord result2 = smt.apply(record2);

      assertThat(result1.valueSchema()).isSameAs(result2.valueSchema());
    }
  }

  @Test
  public void testRecordMetadataPreserved() {
    try (EnvelopeTransform<SinkRecord> smt = new EnvelopeTransform<>()) {
      smt.configure(Collections.emptyMap());

      Schema keySchema = Schema.STRING_SCHEMA;
      String key = "my-key";
      Schema valueSchema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();
      Struct value = new Struct(valueSchema).put("id", 1L);

      long kafkaTs = 1700000000000L;
      SinkRecord record =
          new SinkRecord("my-topic", 3, keySchema, key, valueSchema, value, 42, kafkaTs, null);
      SinkRecord result = smt.apply(record);

      assertThat(result.topic()).isEqualTo("my-topic");
      assertThat(result.kafkaPartition()).isEqualTo(3);
      assertThat(result.keySchema()).isEqualTo(keySchema);
      assertThat(result.key()).isEqualTo("my-key");
      assertThat(result.timestamp()).isEqualTo(kafkaTs);
    }
  }
}

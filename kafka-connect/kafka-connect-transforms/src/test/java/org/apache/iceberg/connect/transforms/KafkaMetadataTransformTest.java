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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class KafkaMetadataTransformTest {

  private static final Schema SCHEMA = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA);
  private static final Struct VALUE_STRUCT = new Struct(SCHEMA).put("id", "value");
  private static final Map<String, Object> VALUE_MAP = ImmutableMap.of("id", "value");
  private static final String TOPIC = "topic";
  private static final int PARTITION = 0;
  private static final long OFFSET = 1000L;
  private static final long TIMESTAMP = 50000L;
  private static final Schema KEY_SCHEMA = SchemaBuilder.STRING_SCHEMA;
  private static final String KEY_VALUE = "key";

  @Test
  @DisplayName("should pass through null records as-is")
  public void testNullRecord() {
    SinkRecord record =
        new SinkRecord(
            TOPIC, PARTITION, null, null, null, null, OFFSET, TIMESTAMP, TimestampType.CREATE_TIME);
    try (KafkaMetadataTransform smt = new KafkaMetadataTransform()) {
      smt.configure(ImmutableMap.of());
      SinkRecord result = smt.apply(record);
      assertThat(record).isSameAs(result);
    }
  }

  @Test
  @DisplayName("should throw if value is not struct or map")
  public void testThrowIfNotExpectedValue() {
    SinkRecord recordNotMap =
        new SinkRecord(
            TOPIC,
            PARTITION,
            null,
            null,
            null,
            "not a map",
            OFFSET,
            TIMESTAMP,
            TimestampType.CREATE_TIME);
    SinkRecord recordNotStruct =
        new SinkRecord(
            TOPIC,
            PARTITION,
            null,
            null,
            SCHEMA,
            "not a struct",
            OFFSET,
            TIMESTAMP,
            TimestampType.CREATE_TIME);
    try (KafkaMetadataTransform smt = new KafkaMetadataTransform()) {
      smt.configure(ImmutableMap.of());
      assertThatThrownBy(() -> smt.apply(recordNotMap)).isInstanceOf(RuntimeException.class);
      assertThatThrownBy(() -> smt.apply(recordNotStruct)).isInstanceOf(RuntimeException.class);
    }
  }

  @Test
  @DisplayName("should append kafka metadata to structs")
  public void testAppendsToStucts() {
    SinkRecord record =
        new SinkRecord(
            TOPIC,
            PARTITION,
            KEY_SCHEMA,
            KEY_VALUE,
            SCHEMA,
            VALUE_STRUCT,
            OFFSET,
            TIMESTAMP,
            TimestampType.CREATE_TIME);
    try (KafkaMetadataTransform smt = new KafkaMetadataTransform()) {
      smt.configure(ImmutableMap.of("field_name", "_some_field"));
      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Struct.class);
      Struct value = (Struct) result.value();
      assertThat(value.get("id")).isEqualTo("value");
      assertThat(value.get("_some_field_topic")).isEqualTo(result.topic());
      assertThat(value.get("_some_field_partition")).isEqualTo(result.kafkaPartition());
      assertThat(value.get("_some_field_offset")).isEqualTo(result.kafkaOffset());
      assertThat(value.get("_some_field_timestamp")).isEqualTo(result.timestamp());
      assertThat(result.timestampType()).isEqualTo(record.timestampType());
      assertThat(result.key()).isEqualTo(record.key());
      assertThat(result.keySchema()).isEqualTo(record.keySchema());
    }
  }

  @Test
  @DisplayName("should append kafka metadata to nested structs")
  public void testAppendsToStructsNested() {
    SinkRecord record =
        new SinkRecord(
            TOPIC,
            PARTITION,
            KEY_SCHEMA,
            KEY_VALUE,
            SCHEMA,
            VALUE_STRUCT,
            OFFSET,
            TIMESTAMP,
            TimestampType.CREATE_TIME);
    try (KafkaMetadataTransform smt = new KafkaMetadataTransform()) {
      smt.configure(ImmutableMap.of("nested", "true"));
      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Struct.class);
      Struct value = (Struct) result.value();
      assertThat(value.get("_kafka_metadata")).isInstanceOf(Struct.class);
      Struct metadata = (Struct) value.get("_kafka_metadata");
      assertThat(metadata.get("topic")).isEqualTo(result.topic());
      assertThat(metadata.get("partition")).isEqualTo(result.kafkaPartition());
      assertThat(metadata.get("offset")).isEqualTo(result.kafkaOffset());
      assertThat(metadata.get("timestamp")).isEqualTo(result.timestamp());
      assertThat(result.timestampType()).isEqualTo(record.timestampType());
      assertThat(result.key()).isEqualTo(record.key());
      assertThat(result.keySchema()).isEqualTo(record.keySchema());
    }
  }

  @Test
  @DisplayName("should append external fields to struct")
  public void testAppendsToStuctsExternal() {
    SinkRecord record =
        new SinkRecord(
            TOPIC,
            PARTITION,
            KEY_SCHEMA,
            KEY_VALUE,
            SCHEMA,
            VALUE_STRUCT,
            OFFSET,
            TIMESTAMP,
            TimestampType.CREATE_TIME);
    try (KafkaMetadataTransform smt = new KafkaMetadataTransform()) {
      smt.configure(ImmutableMap.of("external_field", "external,value"));
      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Struct.class);
      Struct value = (Struct) result.value();
      assertThat(value.get("id")).isEqualTo("value");
      assertThat(value.get("_kafka_metadata_topic")).isEqualTo(result.topic());
      assertThat(value.get("_kafka_metadata_partition")).isEqualTo(result.kafkaPartition());
      assertThat(value.get("_kafka_metadata_offset")).isEqualTo(result.kafkaOffset());
      assertThat(value.get("_kafka_metadata_timestamp")).isEqualTo(result.timestamp());
      assertThat(value.get("_kafka_metadata_external")).isEqualTo("value");
    }
  }

  @Test
  @DisplayName("throw if external field cannot be parsed")
  public void testAppendsToStuctsExternalShouldThrowIfInvalid() {
    try (KafkaMetadataTransform smt = new KafkaMetadataTransform()) {

      assertThatThrownBy(
              () -> {
                smt.configure(ImmutableMap.of("external_field", "external,*,,,value"));
              })
          .isInstanceOf(RuntimeException.class);
    }
  }

  @Test
  @DisplayName("should append kafka metadata to maps")
  public void testAppendToMaps() {
    SinkRecord record =
        new SinkRecord(
            TOPIC,
            PARTITION,
            null,
            null,
            null,
            VALUE_MAP,
            OFFSET,
            TIMESTAMP,
            TimestampType.CREATE_TIME);
    try (KafkaMetadataTransform smt = new KafkaMetadataTransform()) {
      smt.configure(ImmutableMap.of());
      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Map.class);
      Map<?, ?> value = (Map<?, ?>) result.value();
      assertThat(value.get("id")).isEqualTo("value");
      assertThat(value.get("_kafka_metadata_topic")).isEqualTo(result.topic());
      assertThat(value.get("_kafka_metadata_partition")).isEqualTo(result.kafkaPartition());
      assertThat(value.get("_kafka_metadata_offset")).isEqualTo(result.kafkaOffset());
      assertThat(value.get("_kafka_metadata_timestamp")).isEqualTo(result.timestamp());
      assertThat(result.timestampType()).isEqualTo(record.timestampType());
      assertThat(result.key()).isEqualTo(record.key());
      assertThat(result.keySchema()).isEqualTo(record.keySchema());
    }
  }

  @Test
  @DisplayName("should append kafka metadata to maps as nested")
  public void testAppendToMapsNested() {
    SinkRecord record =
        new SinkRecord(
            TOPIC,
            PARTITION,
            null,
            null,
            null,
            VALUE_MAP,
            OFFSET,
            TIMESTAMP,
            TimestampType.CREATE_TIME);
    try (KafkaMetadataTransform smt = new KafkaMetadataTransform()) {
      smt.configure(ImmutableMap.of("nested", "true"));
      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Map.class);
      Map<?, ?> value = (Map<?, ?>) result.value();
      assertThat(value.get("_kafka_metadata")).isInstanceOf(Map.class);
      Map<?, ?> metadata = (Map<?, ?>) value.get("_kafka_metadata");
      assertThat(metadata.get("topic")).isEqualTo(result.topic());
      assertThat(metadata.get("partition")).isEqualTo(result.kafkaPartition());
      assertThat(metadata.get("offset")).isEqualTo(result.kafkaOffset());
      assertThat(metadata.get("timestamp")).isEqualTo(result.timestamp());
      assertThat(result.timestampType()).isEqualTo(record.timestampType());
      assertThat(result.key()).isEqualTo(record.key());
      assertThat(result.keySchema()).isEqualTo(record.keySchema());
    }
  }
}

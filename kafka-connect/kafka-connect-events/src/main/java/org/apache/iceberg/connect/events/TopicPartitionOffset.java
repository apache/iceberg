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
package org.apache.iceberg.connect.events;

import java.time.OffsetDateTime;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.util.DateTimeUtil;

/** Element representing an offset, with topic name, partition number, and offset. */
public class TopicPartitionOffset implements IndexedRecord {

  private String topic;
  private Integer partition;
  private Long offset;
  private OffsetDateTime timestamp;
  private final Schema avroSchema;
  private final int[] positionsToIds;

  static final int TOPIC = 10_700;
  static final int PARTITION = 10_701;
  static final int OFFSET = 10_702;
  static final int TIMESTAMP = 10_703;

  public static final StructType ICEBERG_SCHEMA =
      StructType.of(
          NestedField.required(TOPIC, "topic", StringType.get()),
          NestedField.required(PARTITION, "partition", IntegerType.get()),
          NestedField.optional(OFFSET, "offset", LongType.get()),
          NestedField.optional(TIMESTAMP, "timestamp", TimestampType.withZone()));
  private static final Schema AVRO_SCHEMA =
      AvroUtil.convert(ICEBERG_SCHEMA, TopicPartitionOffset.class);
  private static final int[] POSITIONS_TO_IDS = AvroUtil.positionsToIds(AVRO_SCHEMA);

  // Used by Avro reflection to instantiate this class when reading events
  public TopicPartitionOffset(Schema avroSchema) {
    this.avroSchema = avroSchema;
    this.positionsToIds = AvroUtil.positionsToIds(avroSchema);
  }

  public TopicPartitionOffset(String topic, int partition, Long offset, OffsetDateTime timestamp) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.avroSchema = AVRO_SCHEMA;
    this.positionsToIds = POSITIONS_TO_IDS;
  }

  public String topic() {
    return topic;
  }

  public Integer partition() {
    return partition;
  }

  public Long offset() {
    return offset;
  }

  public OffsetDateTime timestamp() {
    return timestamp;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    switch (positionsToIds[i]) {
      case TOPIC:
        this.topic = v == null ? null : v.toString();
        return;
      case PARTITION:
        this.partition = (Integer) v;
        return;
      case OFFSET:
        this.offset = (Long) v;
        return;
      case TIMESTAMP:
        this.timestamp = v == null ? null : DateTimeUtil.timestamptzFromMicros((Long) v);
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (positionsToIds[i]) {
      case TOPIC:
        return topic;
      case PARTITION:
        return partition;
      case OFFSET:
        return offset;
      case TIMESTAMP:
        return timestamp == null ? null : DateTimeUtil.microsFromTimestamptz(timestamp);
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}

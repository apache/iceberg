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
package io.tabular.iceberg.connect.events;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class TopicPartitionOffset implements Element {

  private String topic;
  private Integer partition;
  private Long offset;
  private Long timestamp;
  private final Schema avroSchema;

  public static final Schema AVRO_SCHEMA =
      SchemaBuilder.builder()
          .record(TopicPartitionOffset.class.getName())
          .fields()
          .name("topic")
          .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
          .type()
          .stringType()
          .noDefault()
          .name("partition")
          .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
          .type()
          .intType()
          .noDefault()
          .name("offset")
          .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
          .type()
          .nullable()
          .longType()
          .noDefault()
          .name("timestamp")
          .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
          .type()
          .nullable()
          .longType()
          .noDefault()
          .endRecord();

  // Used by Avro reflection to instantiate this class when reading events
  public TopicPartitionOffset(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public TopicPartitionOffset(String topic, int partition, Long offset, Long timestamp) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.avroSchema = AVRO_SCHEMA;
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

  public Long timestamp() {
    return timestamp;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.topic = v == null ? null : v.toString();
        return;
      case 1:
        this.partition = (Integer) v;
        return;
      case 2:
        this.offset = (Long) v;
        return;
      case 3:
        this.timestamp = (Long) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return topic;
      case 1:
        return partition;
      case 2:
        return offset;
      case 3:
        return timestamp;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}

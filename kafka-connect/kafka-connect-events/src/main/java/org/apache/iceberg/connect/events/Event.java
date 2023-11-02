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
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.types.Types.UUIDType;
import org.apache.iceberg.util.DateTimeUtil;

/**
 * Class representing all events produced to the control topic. Different event types have different
 * payloads.
 */
public class Event implements IndexedRecord {

  private UUID id;
  private PayloadType type;
  private OffsetDateTime timestamp;
  private String groupId;
  private Payload payload;
  private final Schema avroSchema;

  // Used by Avro reflection to instantiate this class when reading events
  public Event(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public Event(String groupId, Payload payload) {
    this.id = UUID.randomUUID();
    this.type = payload.type();
    this.timestamp = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
    this.groupId = groupId;
    this.payload = payload;

    StructType icebergSchema =
        StructType.of(
            NestedField.required(10_500, "id", UUIDType.get()),
            NestedField.required(10_501, "type", IntegerType.get()),
            NestedField.required(10_502, "timestamp", TimestampType.withZone()),
            NestedField.required(10_503, "group_id", StringType.get()),
            NestedField.required(10_504, "payload", payload.writeSchema()));

    Map<Integer, String> typeMap = Maps.newHashMap(AvroUtil.FIELD_ID_TO_CLASS);
    typeMap.put(10_504, payload.getClass().getName());

    this.avroSchema = AvroUtil.convert(icebergSchema, getClass(), typeMap);
  }

  public UUID id() {
    return id;
  }

  public PayloadType type() {
    return type;
  }

  public OffsetDateTime timestamp() {
    return timestamp;
  }

  public Payload payload() {
    return payload;
  }

  public String groupId() {
    return groupId;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.id = (UUID) v;
        return;
      case 1:
        this.type = v == null ? null : PayloadType.values()[(Integer) v];
        return;
      case 2:
        this.timestamp = v == null ? null : DateTimeUtil.timestamptzFromMicros((Long) v);
        return;
      case 3:
        this.groupId = v == null ? null : v.toString();
        return;
      case 4:
        this.payload = (Payload) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return id;
      case 1:
        return type == null ? null : type.id();
      case 2:
        return timestamp == null ? null : DateTimeUtil.microsFromTimestamptz(timestamp);
      case 3:
        return groupId;
      case 4:
        return payload;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}

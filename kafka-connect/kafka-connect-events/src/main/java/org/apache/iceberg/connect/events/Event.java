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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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

  static final int ID = 10_500;
  static final int TYPE = 10_501;
  static final int TIMESTAMP = 10_502;
  static final int GROUP_ID = 10_503;
  static final int PAYLOAD = 10_504;

  // Used by Avro reflection to instantiate this class when reading events
  public Event(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public Event(String groupId, Payload payload) {
    Preconditions.checkNotNull(groupId, "Group ID cannot be null");
    Preconditions.checkNotNull(payload, "Payload cannot be null");

    this.id = UUID.randomUUID();
    this.type = payload.type();
    this.timestamp = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
    this.groupId = groupId;
    this.payload = payload;

    StructType icebergSchema =
        StructType.of(
            NestedField.required(ID, "id", UUIDType.get()),
            NestedField.required(TYPE, "type", IntegerType.get()),
            NestedField.required(TIMESTAMP, "timestamp", TimestampType.withZone()),
            NestedField.required(GROUP_ID, "group_id", StringType.get()),
            NestedField.required(PAYLOAD, "payload", payload.writeSchema()));

    Map<Integer, String> typeMap = Maps.newHashMap(AvroUtil.FIELD_ID_TO_CLASS);
    typeMap.put(PAYLOAD, payload.getClass().getName());

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
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case ID:
        this.id = (UUID) v;
        return;
      case TYPE:
        this.type = v == null ? null : PayloadType.values()[(Integer) v];
        return;
      case TIMESTAMP:
        this.timestamp = v == null ? null : DateTimeUtil.timestamptzFromMicros((Long) v);
        return;
      case GROUP_ID:
        this.groupId = v == null ? null : v.toString();
        return;
      case PAYLOAD:
        this.payload = (Payload) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case ID:
        return id;
      case TYPE:
        return type == null ? null : type.id();
      case TIMESTAMP:
        return timestamp == null ? null : DateTimeUtil.microsFromTimestamptz(timestamp);
      case GROUP_ID:
        return groupId;
      case PAYLOAD:
        return payload;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}

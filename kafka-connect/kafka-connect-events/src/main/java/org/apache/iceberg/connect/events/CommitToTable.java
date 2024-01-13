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
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.types.Types.UUIDType;
import org.apache.iceberg.util.DateTimeUtil;

/**
 * A control event payload for events sent by a coordinator that indicates it has completed a commit
 * cycle. Events with this payload are not consumed by the sink, they are informational and can be
 * used by consumers to trigger downstream processes.
 */
public class CommitToTable implements Payload {

  private UUID commitId;
  private TableReference tableReference;
  private Long snapshotId;
  private OffsetDateTime validThroughTs;
  private final Schema avroSchema;

  static final int COMMIT_ID = 10_400;
  static final int TABLE_REFERENCE = 10_401;
  static final int SNAPSHOT_ID = 10_402;
  static final int VALID_THROUGH_TS = 10_403;

  private static final StructType ICEBERG_SCHEMA =
      StructType.of(
          NestedField.required(COMMIT_ID, "commit_id", UUIDType.get()),
          NestedField.required(TABLE_REFERENCE, "table_reference", TableReference.ICEBERG_SCHEMA),
          NestedField.optional(SNAPSHOT_ID, "snapshot_id", LongType.get()),
          NestedField.optional(VALID_THROUGH_TS, "valid_through_ts", TimestampType.withZone()));
  private static final Schema AVRO_SCHEMA = AvroUtil.convert(ICEBERG_SCHEMA, CommitToTable.class);

  // Used by Avro reflection to instantiate this class when reading events
  public CommitToTable(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public CommitToTable(
      UUID commitId,
      TableReference tableReference,
      Long snapshotId,
      OffsetDateTime validThroughTs) {
    Preconditions.checkNotNull(commitId, "Commit ID cannot be null");
    Preconditions.checkNotNull(tableReference, "Table reference cannot be null");
    this.commitId = commitId;
    this.tableReference = tableReference;
    this.snapshotId = snapshotId;
    this.validThroughTs = validThroughTs;
    this.avroSchema = AVRO_SCHEMA;
  }

  @Override
  public PayloadType type() {
    return PayloadType.COMMIT_TO_TABLE;
  }

  public UUID commitId() {
    return commitId;
  }

  public TableReference tableReference() {
    return tableReference;
  }

  public Long snapshotId() {
    return snapshotId;
  }

  public OffsetDateTime validThroughTs() {
    return validThroughTs;
  }

  @Override
  public StructType writeSchema() {
    return ICEBERG_SCHEMA;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case COMMIT_ID:
        this.commitId = (UUID) v;
        return;
      case TABLE_REFERENCE:
        this.tableReference = (TableReference) v;
        return;
      case SNAPSHOT_ID:
        this.snapshotId = (Long) v;
        return;
      case VALID_THROUGH_TS:
        this.validThroughTs = v == null ? null : DateTimeUtil.timestamptzFromMicros((Long) v);
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case COMMIT_ID:
        return commitId;
      case TABLE_REFERENCE:
        return tableReference;
      case SNAPSHOT_ID:
        return snapshotId;
      case VALID_THROUGH_TS:
        return validThroughTs == null ? null : DateTimeUtil.microsFromTimestamptz(validThroughTs);
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}

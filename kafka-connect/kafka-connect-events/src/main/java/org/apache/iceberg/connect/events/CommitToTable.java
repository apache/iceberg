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

import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.types.Types.UUIDType;

/**
 * A control event payload for events sent by a coordinator that indicates it has completed a commit
 * cycle. Events with this payload are not consumed by the sink, they are informational and can be
 * used by consumers to trigger downstream processes.
 */
public class CommitToTable implements Payload {

  private UUID commitId;
  private TableReference tableReference;
  private Long snapshotId;
  private Long validThroughTs;
  private final Schema avroSchema;

  private static final StructType ICEBERG_SCHEMA =
      StructType.of(
          NestedField.required(10_400, "commit_id", UUIDType.get()),
          NestedField.required(10_401, "table_reference", TableReference.ICEBERG_SCHEMA),
          NestedField.optional(10_402, "snapshot_id", LongType.get()),
          NestedField.optional(10_403, "valid_through_ts", TimestampType.withZone()));

  private static final Schema AVRO_SCHEMA = AvroUtil.convert(ICEBERG_SCHEMA);

  // Used by Avro reflection to instantiate this class when reading events
  public CommitToTable(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public CommitToTable(
      UUID commitId, TableReference tableReference, Long snapshotId, Long validThroughTs) {
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

  public Long validThroughTs() {
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
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.commitId = (UUID) v;
        return;
      case 1:
        this.tableReference = (TableReference) v;
        return;
      case 2:
        this.snapshotId = (Long) v;
        return;
      case 3:
        this.validThroughTs = (Long) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return commitId;
      case 1:
        return tableReference;
      case 2:
        return snapshotId;
      case 3:
        return validThroughTs;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}

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
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.UUIDType;

/**
 * A control event payload for events sent by a coordinator to request workers to send back the
 * table data that has been written and is ready to commit.
 */
public class StartCommit implements Payload {

  private UUID commitId;
  private final Schema avroSchema;

  static final int COMMIT_ID = 10_200;

  private static final StructType ICEBERG_SCHEMA =
      StructType.of(NestedField.required(COMMIT_ID, "commit_id", UUIDType.get()));
  private static final Schema AVRO_SCHEMA = AvroUtil.convert(ICEBERG_SCHEMA, StartCommit.class);

  // Used by Avro reflection to instantiate this class when reading events
  public StartCommit(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public StartCommit(UUID commitId) {
    this.commitId = commitId;
    this.avroSchema = AVRO_SCHEMA;
  }

  @Override
  public PayloadType type() {
    return PayloadType.START_COMMIT;
  }

  public UUID commitId() {
    return commitId;
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
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case COMMIT_ID:
        return commitId;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}

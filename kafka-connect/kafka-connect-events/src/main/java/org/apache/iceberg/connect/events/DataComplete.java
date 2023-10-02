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

import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.UUIDType;

/**
 * A control event payload for events sent by a worker that indicates it has finished sending all
 * data for a commit request.
 */
public class DataComplete implements Payload {

  private UUID commitId;
  private List<TopicPartitionOffset> assignments;
  private final Schema avroSchema;

  private static final StructType ICEBERG_SCHEMA =
      StructType.of(
          NestedField.required(10_100, "commit_id", UUIDType.get()),
          NestedField.optional(
              10_101,
              "assignments",
              ListType.ofRequired(10_102, TopicPartitionOffset.ICEBERG_SCHEMA)));

  private static final Schema AVRO_SCHEMA = AvroUtil.convert(ICEBERG_SCHEMA);

  // Used by Avro reflection to instantiate this class when reading events
  public DataComplete(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public DataComplete(UUID commitId, List<TopicPartitionOffset> assignments) {
    this.commitId = commitId;
    this.assignments = assignments;
    this.avroSchema = AVRO_SCHEMA;
  }

  @Override
  public PayloadType type() {
    return PayloadType.DATA_COMPLETE;
  }

  public UUID commitId() {
    return commitId;
  }

  public List<TopicPartitionOffset> assignments() {
    return assignments;
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
        this.assignments = (List<TopicPartitionOffset>) v;
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
        return assignments;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}

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
package org.apache.iceberg;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class GenericBlobMetadata implements BlobMetadata {

  public static BlobMetadata from(org.apache.iceberg.puffin.BlobMetadata puffinMetadata) {
    return new GenericBlobMetadata(
        puffinMetadata.type(),
        puffinMetadata.snapshotId(),
        puffinMetadata.sequenceNumber(),
        puffinMetadata.inputFields(),
        puffinMetadata.properties());
  }

  private final String type;
  private final long sourceSnapshotId;
  private final long sourceSnapshotSequenceNumber;
  private final List<Integer> fields;
  private final Map<String, String> properties;

  public GenericBlobMetadata(
      String type,
      long sourceSnapshotId,
      long sourceSnapshotSequenceNumber,
      List<Integer> fields,
      Map<String, String> properties) {
    Preconditions.checkNotNull(type, "type is null");
    Preconditions.checkNotNull(fields, "fields is null");
    Preconditions.checkNotNull(properties, "properties is null");
    this.type = type;
    this.sourceSnapshotId = sourceSnapshotId;
    this.sourceSnapshotSequenceNumber = sourceSnapshotSequenceNumber;
    this.fields = ImmutableList.copyOf(fields);
    this.properties = ImmutableMap.copyOf(properties);
  }

  @Override
  public String type() {
    return type;
  }

  @Override
  public long sourceSnapshotId() {
    return sourceSnapshotId;
  }

  @Override
  public long sourceSnapshotSequenceNumber() {
    return sourceSnapshotSequenceNumber;
  }

  @Override
  public List<Integer> fields() {
    return fields;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GenericBlobMetadata that = (GenericBlobMetadata) o;
    return sourceSnapshotId == that.sourceSnapshotId
        && sourceSnapshotSequenceNumber == that.sourceSnapshotSequenceNumber
        && Objects.equals(type, that.type)
        && Objects.equals(fields, that.fields)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, sourceSnapshotId, sourceSnapshotSequenceNumber, fields, properties);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", GenericBlobMetadata.class.getSimpleName() + "[", "]")
        .add("type='" + type + "'")
        .add("sourceSnapshotId=" + sourceSnapshotId)
        .add("sourceSnapshotSequenceNumber=" + sourceSnapshotSequenceNumber)
        .add("fields=" + fields)
        .add("properties=" + properties)
        .toString();
  }
}

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

import java.io.Serializable;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.transforms.Transform;

/** Represents a single field in a {@link PartitionSpec}. */
public class PartitionField implements Serializable {
  private final List<Integer> sourceIds;
  private final int fieldId;
  private final String name;
  private final Transform<?, ?> transform;

  PartitionField(int sourceId, int fieldId, String name, Transform<?, ?> transform) {
    this(ImmutableList.of(sourceId), fieldId, name, transform);
  }

  PartitionField(List<Integer> sourceIds, int fieldId, String name, Transform<?, ?> transform) {
    this.sourceIds = ImmutableList.copyOf(sourceIds);
    this.fieldId = fieldId;
    this.name = name;
    this.transform = transform;
  }

  /** Returns the field id of the source field in the {@link PartitionSpec spec's} table schema. */
  public int sourceId() {
    return sourceIds.get(0);
  }

  /**
   * Returns the field ids of all source fields for this partition field.
   *
   * <p>For single-argument transforms, this list contains one element. For multi-argument
   * transforms, this list contains multiple source field ids.
   */
  public List<Integer> sourceIds() {
    return sourceIds;
  }

  /** Returns the partition field id across all the table metadata's partition specs. */
  public int fieldId() {
    return fieldId;
  }

  /** Returns the name of this partition field. */
  public String name() {
    return name;
  }

  /** Returns the transform used to produce partition values from source values. */
  public Transform<?, ?> transform() {
    return transform;
  }

  @Override
  public String toString() {
    if (sourceIds.size() == 1) {
      return fieldId + ": " + name + ": " + transform + "(" + sourceIds.get(0) + ")";
    }
    return fieldId + ": " + name + ": " + transform + "(" + sourceIds + ")";
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof PartitionField)) {
      return false;
    }

    PartitionField that = (PartitionField) other;
    return sourceIds.equals(that.sourceIds)
        && fieldId == that.fieldId
        && name.equals(that.name)
        && transform.toString().equals(that.transform.toString());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sourceIds, fieldId, name, transform);
  }
}

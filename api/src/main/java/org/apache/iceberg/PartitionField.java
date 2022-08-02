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
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.transforms.Transform;

/** Represents a single field in a {@link PartitionSpec}. */
public class PartitionField implements Serializable {
  private final int sourceId;
  private final int fieldId;
  private final String name;
  private final Transform<?, ?> transform;
  private final int schemaId;

  PartitionField(int sourceId, int fieldId, String name, Transform<?, ?> transform, int schemaId) {
    this.sourceId = sourceId;
    this.fieldId = fieldId;
    this.name = name;
    this.transform = transform;
    this.schemaId = schemaId;
  }

  /** Returns the field id of the source field in the {@link PartitionSpec spec's} table schema. */
  public int sourceId() {
    return sourceId;
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

  /** Returns the schema id of the table schema from which the source field is obtained from. */
  public int schemaId() {
    return schemaId;
  }

  @Override
  public String toString() {
    return fieldId + ": " + name + ": " + transform + "(" + sourceId + ": " + schemaId + ")";
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof PartitionField)) {
      return false;
    }

    PartitionField that = (PartitionField) other;
    return sourceId == that.sourceId
        && fieldId == that.fieldId
        && name.equals(that.name)
        && transform.equals(that.transform)
        && schemaId == (that.schemaId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sourceId, fieldId, name, transform, schemaId);
  }
}

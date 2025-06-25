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
package org.apache.flink.table.connector.source.partitioning;

import org.apache.flink.table.expressions.TransformExpression;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * TODO Consider relaxing this constraint in a future version Preconditions: 1. keys are ordered by
 * the partition columns defined in the table schema. 2. the partition values are ordered by the
 * values in Row, comparing the values from 1st to last. for example: if a table is partitioned by
 * (dt, bucket(128, user_id)) then the partition keys = [dt, bucket(128, user_id)]. It cannot be
 * [bucket(128, user_id), dt]. the partition values can be ("2023-10-01", 0), ("2023-10-01", 1),
 * ("2023-10-02", 0), ... it cannot be ("2023-10-01", 1), ("2023-10-01", 0), ("2023-10-02", 0), ...
 */
// TODO -- remove this once this function can be imported from flink libraries
public class KeyGroupedPartitioning implements Partitioning {
  private final TransformExpression[] keys;
  private final int numPartitions;
  private final Row[] partitionValues;

  // bucket(128, user_id)
  // partitioned by (dt, bucket(128, user_id)
  // dt=2023-10-01/user_id_bucket=0/ => InternalRow("2023-10-01", 0)

  public KeyGroupedPartitioning(
      TransformExpression[] keys, Row[] partitionValues, int numPartitions) {
    this.keys = keys;
    this.numPartitions = numPartitions;
    this.partitionValues = partitionValues;
  }

  /** Returns the partition transform expressions for this partitioning. */
  public TransformExpression[] keys() {
    return keys;
  }

  public Row[] getPartitionValues() {
    return partitionValues;
  }

  @Override
  public int numPartitions() {
    return numPartitions;
  }

  boolean isPartitionedByKeys(String keysString) {
    for (TransformExpression key : keys) {
      if (key.getKey().equals(keysString)) {
        return true;
      }
    }
    return false;
  }

  /**
   * * Checks if this partitioning is compatible with another KeyGroupedPartitioning. conditions: 1.
   * numPartitions is the same 2. keys length is the same and for each key,keys are compatible 3.
   * RowData length is the same. values are the same.
   *
   * @param other the other KeyGroupedPartitioning to check compatibility with
   * @return true if compatible, false otherwise
   */
  public boolean isCompatible(KeyGroupedPartitioning other) {
    if (other == null) {
      return false;
    }

    // 1. Check numPartitions is the same
    if (this.numPartitions != other.numPartitions) {
      return false;
    }

    // 2. Check keys length is the same and each key is compatible
    if (this.keys.length != other.keys.length) {
      return false;
    }

    for (int i = 0; i < this.keys.length; i++) {
      if (!this.keys[i].isCompatible(other.keys[i])) {
        return false;
      }
    }

    // 3. Check RowData length and values are the same
    if (this.partitionValues.length != other.partitionValues.length) {
      return false;
    }

    for (int i = 0; i < this.partitionValues.length; i++) {
      Row thisRow = this.partitionValues[i];
      Row otherRow = other.partitionValues[i];

      if (thisRow.getArity() != otherRow.getArity()) {
        return false;
      }

      for (int j = 0; j < thisRow.getArity(); j++) {
        // filed in row cannot be null
        Preconditions.checkArgument(thisRow.getField(j) != null);
        if (!thisRow.getField(j).equals(otherRow.getField(j))) {
          return false;
        }
      }
    }

    return true;
  }
}

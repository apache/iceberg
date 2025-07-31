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

import org.apache.iceberg.types.Type;

/** Iceberg internally tracked field level metrics. */
public class FieldMetrics<T> {
  private final int id;
  private final long valueCount;
  private final long nullValueCount;
  private final long nanValueCount;
  private final T lowerBound;
  private final T upperBound;
  private final Type originalType;

  public FieldMetrics(int id, long valueCount, long nullValueCount) {
    this(id, valueCount, nullValueCount, -1L, null, null, null);
  }

  public FieldMetrics(int id, long valueCount, long nullValueCount, T lowerBound, T upperBound) {
    this(id, valueCount, nullValueCount, -1L, lowerBound, upperBound, null);
  }

  public FieldMetrics(int id, long valueCount, long nullValueCount, long nanValueCount) {
    this(id, valueCount, nullValueCount, nanValueCount, null, null, null);
  }

  public FieldMetrics(
      int id, long valueCount, long nullValueCount, T lowerBound, T upperBound, Type originalType) {
    this(id, valueCount, nullValueCount, -1L, lowerBound, upperBound, originalType);
  }

  public FieldMetrics(
      int id,
      long valueCount,
      long nullValueCount,
      long nanValueCount,
      T lowerBound,
      T upperBound) {
    this(id, valueCount, nullValueCount, nanValueCount, lowerBound, upperBound, null);
  }

  public FieldMetrics(
      int id,
      long valueCount,
      long nullValueCount,
      long nanValueCount,
      T lowerBound,
      T upperBound,
      Type originalType) {
    this.id = id;
    this.valueCount = valueCount;
    this.nullValueCount = nullValueCount;
    this.nanValueCount = nanValueCount;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.originalType = originalType;
  }

  /** Returns the id of the field that the metrics within this class are associated with. */
  public int id() {
    return id;
  }

  /** Returns the number of all values, including nulls, NaN and repeated, for the given field. */
  public long valueCount() {
    return valueCount;
  }

  /** Returns the number of null values for this field. */
  public long nullValueCount() {
    return nullValueCount;
  }

  /**
   * Returns the number of NaN values for this field. Will only be non-0 if this field is a double
   * or float field.
   */
  public long nanValueCount() {
    return nanValueCount;
  }

  /** Returns the lower bound value of this field. */
  public T lowerBound() {
    return lowerBound;
  }

  /** Returns the upper bound value of this field. */
  public T upperBound() {
    return upperBound;
  }

  /** Returns the original type of the upper/lower bound value of this field. */
  public Type originalType() {
    return originalType;
  }

  /** Returns if the metrics has bounds (i.e. there is at least non-null value for this field) */
  public boolean hasBounds() {
    return upperBound != null;
  }
}

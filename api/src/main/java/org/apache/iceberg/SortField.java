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
import java.util.Arrays;
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.transforms.Transform;

/** A field in a {@link SortOrder}. */
public class SortField implements Serializable {

  private final Transform<?, ?> transform;
  private final int sourceId;
  private final int[] sourceIds;
  private final SortDirection direction;
  private final NullOrder nullOrder;

  SortField(Transform<?, ?> transform, int sourceId, SortDirection direction, NullOrder nullOrder) {
    this.transform = transform;
    this.sourceId = sourceId;
    this.sourceIds = new int[] {sourceId};
    this.direction = direction;
    this.nullOrder = nullOrder;
  }

  SortField(
      Transform<?, ?> transform, int[] sourceIds, SortDirection direction, NullOrder nullOrder) {
    Preconditions.checkArgument(
        sourceIds != null && sourceIds.length >= 1, "at least one source id should be provided");
    this.transform = transform;
    this.sourceId = sourceIds.length > 1 ? -1 : sourceIds[0];
    this.sourceIds = sourceIds;
    this.direction = direction;
    this.nullOrder = nullOrder;
  }

  /**
   * Returns the transform used to produce sort values from source values.
   *
   * @param <S> the Java type of values transformed by the transform function
   * @param <T> the Java type of values returned by the transform function
   * @return the transform
   */
  @SuppressWarnings("unchecked")
  public <S, T> Transform<S, T> transform() {
    return (Transform<S, T>) transform;
  }

  /** Returns the field id of the source field in the {@link SortOrder sort order's} table schema */
  public int sourceId() {
    return sourceId;
  }

  public int[] sourceIds() {
    return sourceIds;
  }

  /** Returns the sort direction */
  public SortDirection direction() {
    return direction;
  }

  /** Returns the null order */
  public NullOrder nullOrder() {
    return nullOrder;
  }

  /**
   * Checks whether this field's order satisfies another field's order.
   *
   * @param other another sort field
   * @return true if this order satisfies the given order
   */
  public boolean satisfies(SortField other) {
    if (Objects.equals(this, other)) {
      return true;
    } else if (sourceId != other.sourceId
        || !Arrays.equals(sourceIds, other.sourceIds)
        || direction != other.direction
        || nullOrder != other.nullOrder) {
      return false;
    }

    return transform.satisfiesOrderOf(other.transform);
  }

  @Override
  public String toString() {
    if (sourceIds.length == 1) {
      return transform + "(" + sourceId + ") " + direction + " " + nullOrder;
    } else {
      return transform + "(" + Arrays.toString(sourceIds) + ")" + direction + " " + nullOrder;
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    SortField that = (SortField) other;
    return transform.toString().equals(that.transform.toString())
        && sourceId == that.sourceId
        && Arrays.equals(sourceIds, that.sourceIds)
        && direction == that.direction
        && nullOrder == that.nullOrder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(transform, sourceId, direction, nullOrder);
  }
}

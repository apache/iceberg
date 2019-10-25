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
import org.apache.iceberg.SortTransforms.SortTransform;

public class SortField implements Serializable {
  public enum Direction {
    ASC, DESC
  }

  public enum NullOrder {
    NULLS_FIRST, NULLS_LAST
  }

  private final String name;
  private final Direction direction;
  private final NullOrder nullOrder;
  private final SortTransform transform;
  private final int[] sourceIds;

  SortField(String name, Direction direction, NullOrder nullOrder, SortTransform transform, int[] sourceIds) {
    this.name = name;
    this.direction = direction;
    this.nullOrder = nullOrder;
    this.transform = transform;
    this.sourceIds = sourceIds;
  }

  public String name() {
    return name;
  }

  public Direction direction() {
    return direction;
  }

  public NullOrder nullOrder() {
    return nullOrder;
  }

  public SortTransform transform() {
    return transform;
  }

  public int[] sourceIds() {
    return sourceIds;
  }

  public boolean semanticEquals(SortField that) {
    return direction == that.direction &&
        nullOrder == that.nullOrder &&
        Objects.equals(transform, that.transform) &&
        Arrays.equals(sourceIds, that.sourceIds);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    SortField that = (SortField) obj;
    return Objects.equals(name, that.name) &&
        direction == that.direction &&
        nullOrder == that.nullOrder &&
        Objects.equals(transform, that.transform) &&
        Arrays.equals(sourceIds, that.sourceIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, direction, nullOrder, transform, Arrays.hashCode(sourceIds));
  }
}

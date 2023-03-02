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
package org.apache.iceberg.expressions;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types;

public class MinAggregate<T> extends ValueAggregate<T> {
  private final int fieldId;
  private final PrimitiveType type;
  private final Comparator<T> comparator;

  protected MinAggregate(BoundTerm<T> term) {
    super(Operation.MIN, term);
    Types.NestedField field = term.ref().field();
    this.fieldId = field.fieldId();
    this.type = field.type().asPrimitiveType();
    this.comparator = Comparators.forType(type);
  }

  @Override
  protected boolean hasValue(DataFile file) {
    boolean hasBound = file.lowerBounds().containsKey(fieldId);
    Long valueCount = safeGet(file.valueCounts(), fieldId);
    Long nullCount = safeGet(file.nullValueCounts(), fieldId);
    boolean boundAllNull =
        valueCount != null
            && valueCount > 0
            && nullCount != null
            && nullCount.longValue() == valueCount.longValue();
    return hasBound || boundAllNull;
  }

  @Override
  protected Object evaluateRef(DataFile file) {
    return Conversions.fromByteBuffer(type, safeGet(file.lowerBounds(), fieldId));
  }

  @Override
  public Aggregator<T> newAggregator(List<BoundGroupBy<?, ?>> groupBys) {
    return new MinAggregate.MinAggregator<>(this, comparator, groupBys);
  }

  private static class MinAggregator<T> extends NullSafeAggregator<T, T> {
    private final Comparator<T> comparator;
    private T min = null;

    private Map<StructLike, T> minP = Maps.newHashMap();

    MinAggregator(
        MinAggregate<T> aggregate, Comparator<T> comparator, List<BoundGroupBy<?, ?>> groupBys) {
      super(aggregate, groupBys);
      this.comparator = comparator;
    }

    @Override
    protected void update(T value) {
      if (min == null || comparator.compare(value, min) < 0) {
        this.min = value;
      }
    }

    @Override
    protected void update(T value, StructLike partitionKey) {
      if (minP.get(partitionKey) == null || comparator.compare(value, minP.get(partitionKey)) < 0) {
        minP.put(partitionKey, value);
      }
    }

    @Override
    protected T current() {
      return min;
    }

    @Override
    protected Map<StructLike, T> currentPartition() {
      return minP;
    }
  }
}

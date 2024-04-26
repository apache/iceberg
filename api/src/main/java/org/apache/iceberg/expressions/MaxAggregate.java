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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types;

public class MaxAggregate<T> extends ValueAggregate<T> {
  private final int fieldId;
  private final String fieldName;
  private final PrimitiveType type;
  private final Comparator<T> comparator;

  protected MaxAggregate(BoundTerm<T> term) {
    super(Operation.MAX, term);
    Types.NestedField field = term.ref().field();
    this.fieldId = field.fieldId();
    this.fieldName = field.name();
    this.type = field.type().asPrimitiveType();
    this.comparator = Comparators.forType(type);
  }

  @Override
  protected boolean hasValue(DataFile file) {
    boolean hasBound = file.upperBounds().containsKey(fieldId);
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
  protected boolean hasColumnValue(DataFile file) {
    return file.valueCounts().containsKey(fieldId) && file.nullValueCounts().containsKey(fieldId);
  }

  @Override
  protected boolean isIdentityPartitionColumn(StructLike struct) {
    return struct.getPartitionNames().contains(String.valueOf(fieldName));
  }

  @Override
  protected Object evaluateRef(DataFile file) {
    return Conversions.fromByteBuffer(type, safeGet(file.upperBounds(), fieldId));
  }

  @Override
  public Aggregator<T> newAggregator() {
    return new MaxAggregator<>(this, comparator);
  }

  private static class MaxAggregator<T> extends NullSafeAggregator<T, T, T> {
    private final Comparator<T> comparator;
    private T max = null;

    MaxAggregator(MaxAggregate<T> aggregate, Comparator<T> comparator) {
      super(aggregate);
      this.comparator = comparator;
    }

    @Override
    protected void update(T value) {
      if (value == null) {
        return;
      }

      if (max == null || comparator.compare(value, max) > 0) {
        this.max = value;
      }
    }

    @Override
    protected T current() {
      return max;
    }
  }
}

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

import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;

public class CountDistinct<T> extends BoundAggregate<T, T, Integer> {
  private final int fieldId;
  private final String fieldName;
  private final Types.NestedField field;

  protected CountDistinct(BoundTerm<T> term) {
    super(Operation.COUNT_DISTINCT, term);
    this.field = term.ref().field();
    this.fieldId = field.fieldId();
    this.fieldName = field.name();
  }

  @Override
  public Aggregator<Integer> newAggregator() {
    return new CountAggregator<>(this);
  }

  @Override
  public T eval(DataFile file) {
    // return null since data columns doesn't have distinct stats
    return null;
  }

  @Override
  public T eval(StructLike struct) {
    return term().eval(struct);
  }

  protected T countFor(StructLike row) {
    return term().eval(row);
  }

  @Override
  protected boolean hasValue(DataFile file) {
    return file.valueCounts().containsKey(fieldId) && file.nullValueCounts().containsKey(fieldId);
  }

  @Override
  protected boolean hasColumnValue(DataFile file) {
    // return false since data columns doesn't have distinct stats
    return false;
  }

  @Override
  protected boolean isIdentityPartitionColumn(StructLike struct) {
    return struct.getPartitionNames().contains(String.valueOf(fieldName));
  }

  protected T countFor(DataFile file, StructLike row) {
    return countFor(row);
  }

  private static class CountAggregator<T> extends NullSafeAggregator<T, T, Integer> {
    private Set<T> distinctSet = Sets.newHashSet();

    CountAggregator(BoundAggregate<T, T, Integer> aggregate) {
      super(aggregate);
    }

    @Override
    protected void update(T value) {
      if (value != null) {
        distinctSet.add(value);
      }
    }

    @Override
    protected Integer current() {
      // Set.size() returns Integer which maximizes at Integer.MAX_VALUE.
      // Practically there cannot be this many partition files in a single iceberg table.
      return distinctSet.size();
    }
  }
}

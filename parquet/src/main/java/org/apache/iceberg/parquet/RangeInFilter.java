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
package org.apache.iceberg.parquet;

import java.io.Serializable;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.function.Supplier;
import org.apache.iceberg.expressions.RangeInPredUtil;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;

public class RangeInFilter<T extends Comparable<T>> extends UserDefinedPredicate<T>
    implements Serializable {
  private final NavigableSet<T> rangeSet;
  private final Comparator<T> comparator;

  public RangeInFilter(NavigableSet<T> rangeSet, Comparator<T> comparator) {
    this.rangeSet = rangeSet;
    this.comparator = comparator;
  }

  @Override
  public boolean keep(T value) {
    return rangeSet.contains(value);
  }

  @Override
  public boolean canDrop(Statistics<T> statistics) {
    Supplier<T> lowerBoundsSupplier = statistics::getMin;
    Supplier<T> upperBoundsSupplier = statistics::getMax;
    return !RangeInPredUtil.isInRange(
        lowerBoundsSupplier, upperBoundsSupplier, rangeSet, true, this.comparator);
  }

  @Override
  public boolean inverseCanDrop(Statistics statistics) {
    throw new UnsupportedOperationException("Not implemented");
  }
}

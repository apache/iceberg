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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;

/**
 * Helper methods for creating and merging Parquet {@link RowRanges}.
 */
public class ParquetRanges {
  private ParquetRanges() {
  }

  private static final DynConstructors.Ctor<RowRanges.Range> RANGE_CTOR = DynConstructors.builder()
      .hiddenImpl(RowRanges.Range.class, Long.TYPE, Long.TYPE)
      .build();

  public static RowRanges.Range rangeOf(long start, long end) {
    return RANGE_CTOR.newInstance(start, end);
  }

  private static final DynConstructors.Ctor<RowRanges> RANGES_LIST_CTOR = DynConstructors.builder()
      .hiddenImpl(RowRanges.class, List.class)
      .build();

  private static final RowRanges EMPTY = RANGES_LIST_CTOR.newInstance(ImmutableList.of());

  public static RowRanges empty() {
    return EMPTY;
  }

  public static RowRanges of(long start, long end) {
    return of(rangeOf(start, end));
  }

  public static RowRanges of(RowRanges.Range... ranges) {
    return of(Lists.newArrayList(ranges));
  }

  public static RowRanges of(List<RowRanges.Range> ranges) {
    return RANGES_LIST_CTOR.newInstance(ranges);
  }

  private static final DynMethods.StaticMethod UNION = DynMethods.builder("union")
      .impl(RowRanges.class, RowRanges.class, RowRanges.class)
      .buildStatic();

  public static RowRanges union(RowRanges left, RowRanges right) {
    return UNION.invoke(left, right);
  }

  private static final DynMethods.StaticMethod INTERSECTION = DynMethods.builder("intersection")
      .impl(RowRanges.class, RowRanges.class, RowRanges.class)
      .buildStatic();

  public static RowRanges intersection(RowRanges left, RowRanges right) {
    return INTERSECTION.invoke(left, right);
  }

  private static final DynFields.UnboundField BLOCK_ROW_RANGES = DynFields.builder()
      .hiddenImpl(ParquetFileReader.class, "blockRowRanges")
      .build();

  @SuppressWarnings("unchecked")
  public static void setRanges(ParquetFileReader reader, int rowGroupIndex, RowRanges ranges) {
    Map<Integer, RowRanges> rowGroupToRanges = (Map<Integer, RowRanges>) BLOCK_ROW_RANGES.get(reader);
    rowGroupToRanges.put(rowGroupIndex, ranges);
  }
}

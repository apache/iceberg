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
import java.util.PrimitiveIterator;
import java.util.function.IntPredicate;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;

/**
 * Helper methods for page skipping.
 */
class PageSkippingHelpers {
  private PageSkippingHelpers() {
  }

  private static final DynConstructors.Ctor<RowRanges> RANGES_LIST_CTOR = DynConstructors.builder()
      .hiddenImpl(RowRanges.class, List.class)
      .build();

  private static final RowRanges EMPTY = RANGES_LIST_CTOR.newInstance(ImmutableList.of());

  static RowRanges empty() {
    return EMPTY;
  }

  private static final DynMethods.StaticMethod UNION = DynMethods.builder("union")
      .hiddenImpl(RowRanges.class, RowRanges.class, RowRanges.class)
      .buildStatic();

  static RowRanges union(RowRanges left, RowRanges right) {
    return UNION.invoke(left, right);
  }

  private static final DynMethods.StaticMethod INTERSECTION = DynMethods.builder("intersection")
      .hiddenImpl(RowRanges.class, RowRanges.class, RowRanges.class)
      .buildStatic();

  static RowRanges intersection(RowRanges left, RowRanges right) {
    return INTERSECTION.invoke(left, right);
  }

  private static final DynMethods.StaticMethod ROW_RANGES_CREATE =  DynMethods.builder("create")
      .hiddenImpl(RowRanges.class, long.class, PrimitiveIterator.OfInt.class, OffsetIndex.class)
      .buildStatic();

  static RowRanges createRowRanges(long rowCount, PrimitiveIterator.OfInt pageIndexes, OffsetIndex offsetIndex) {
    return ROW_RANGES_CREATE.invoke(rowCount, pageIndexes, offsetIndex);
  }

  private static final DynMethods.StaticMethod ROW_RANGES_CREATE_SINGLE =  DynMethods.builder("createSingle")
      .hiddenImpl(RowRanges.class, long.class)
      .buildStatic();

  static RowRanges allRows(long rowCount) {
    return ROW_RANGES_CREATE_SINGLE.invoke(rowCount);
  }

  private static final DynMethods.StaticMethod INDEX_ITERATOR_ALL = DynMethods.builder("all")
      .hiddenImpl("org.apache.parquet.internal.column.columnindex.IndexIterator", int.class)
      .buildStatic();

  static PrimitiveIterator.OfInt allPageIndexes(int pageCount) {
    return INDEX_ITERATOR_ALL.invoke(pageCount);
  }

  private static final DynMethods.StaticMethod INDEX_ITERATOR_FILTER = DynMethods.builder("filter")
      .hiddenImpl("org.apache.parquet.internal.column.columnindex.IndexIterator", int.class, IntPredicate.class)
      .buildStatic();

  static PrimitiveIterator.OfInt filterPageIndexes(int pageCount, IntPredicate filter) {
    return INDEX_ITERATOR_FILTER.invoke(pageCount, filter);
  }

  private static final DynMethods.UnboundMethod GET_COLUMN_INDEX_STORE =
      DynMethods.builder("getColumnIndexStore")
          .hiddenImpl("org.apache.parquet.hadoop.ParquetFileReader", int.class)
          .build();

  static ColumnIndexStore getColumnIndexStore(ParquetFileReader reader, int blockIndex) {
    return GET_COLUMN_INDEX_STORE.invoke(reader, blockIndex);
  }

  private static final DynMethods.UnboundMethod INTERNAL_READ_FILTERED_ROW_GROUP =
      DynMethods.builder("internalReadFilteredRowGroup")
          .hiddenImpl("org.apache.parquet.hadoop.ParquetFileReader",
              BlockMetaData.class, RowRanges.class, ColumnIndexStore.class)
          .build();

  static PageReadStore internalReadFilteredRowGroup(ParquetFileReader reader, int blockIndex, RowRanges rowRanges) {
    ColumnIndexStore columnIndexStore = GET_COLUMN_INDEX_STORE.invoke(reader, blockIndex);
    BlockMetaData blockMetaData = reader.getRowGroups().get(blockIndex);
    return INTERNAL_READ_FILTERED_ROW_GROUP.invoke(reader, blockMetaData, rowRanges, columnIndexStore);
  }
}

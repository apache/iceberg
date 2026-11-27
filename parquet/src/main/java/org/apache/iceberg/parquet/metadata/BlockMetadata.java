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
package org.apache.iceberg.parquet.metadata;

import java.util.List;
import java.util.Objects;
import org.apache.parquet.format.SortingColumn;

public final class BlockMetadata {
  private final long rowCount;
  private final long totalByteSize;
  private final int ordinal;
  private final List<ColumnChunkMetadata> columns;
  private final List<SortingColumn> sortingColumns;

  public BlockMetadata(long rowCount, List<ColumnChunkMetadata> columns) {
    this(
        rowCount,
        columns.stream().mapToLong(ColumnChunkMetadata::totalUncompressedSize).sum(),
        -1,
        columns);
  }

  public BlockMetadata(
      long rowCount, long totalByteSize, int ordinal, List<ColumnChunkMetadata> columns) {
    this(rowCount, totalByteSize, ordinal, columns, null);
  }

  public BlockMetadata(
      long rowCount,
      long totalByteSize,
      int ordinal,
      List<ColumnChunkMetadata> columns,
      List<SortingColumn> sortingColumns) {
    this.sortingColumns = sortingColumns;
    this.rowCount = rowCount;
    this.totalByteSize = totalByteSize;
    this.ordinal = ordinal;
    this.columns = columns;
  }

  /** Returns the declared sort order of this row group, or null if none is declared. */
  public List<SortingColumn> sortingColumns() {
    return sortingColumns;
  }

  public long totalByteSize() {
    return totalByteSize;
  }

  /** Returns the row group position in the file, or -1 if unknown. */
  public int ordinal() {
    return ordinal;
  }

  public long rowCount() {
    return rowCount;
  }

  public List<ColumnChunkMetadata> columns() {
    return columns;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    BlockMetadata that = (BlockMetadata) obj;
    return this.rowCount == that.rowCount
        && this.totalByteSize == that.totalByteSize
        && this.ordinal == that.ordinal
        && Objects.equals(this.columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowCount, totalByteSize, ordinal, columns);
  }

  @Override
  public String toString() {
    return "BlockMetadata[" + "rowCount=" + rowCount + ", " + "columns=" + columns + ']';
  }
}

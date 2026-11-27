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
import org.apache.parquet.format.GeospatialStatistics;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.SizeStatistics;

/** Locations of the optional column index, offset index and bloom filter of a column chunk. */
public final class ColumnChunkExtras {
  public static final ColumnChunkExtras NONE = new ColumnChunkExtras(-1, -1, -1, -1, -1, -1);

  private final long bloomFilterOffset;
  private final int bloomFilterLength;
  private final long columnIndexOffset;
  private final int columnIndexLength;
  private final long offsetIndexOffset;
  private final int offsetIndexLength;
  private final SizeStatistics sizeStatistics;
  private final GeospatialStatistics geospatialStatistics;
  private final List<KeyValue> keyValueMetadata;

  public ColumnChunkExtras(
      long bloomFilterOffset,
      int bloomFilterLength,
      long columnIndexOffset,
      int columnIndexLength,
      long offsetIndexOffset,
      int offsetIndexLength) {
    this(
        bloomFilterOffset,
        bloomFilterLength,
        columnIndexOffset,
        columnIndexLength,
        offsetIndexOffset,
        offsetIndexLength,
        null,
        null,
        null);
  }

  public ColumnChunkExtras(
      long bloomFilterOffset,
      int bloomFilterLength,
      long columnIndexOffset,
      int columnIndexLength,
      long offsetIndexOffset,
      int offsetIndexLength,
      SizeStatistics sizeStatistics,
      GeospatialStatistics geospatialStatistics,
      List<KeyValue> keyValueMetadata) {
    this.bloomFilterOffset = bloomFilterOffset;
    this.bloomFilterLength = bloomFilterLength;
    this.columnIndexOffset = columnIndexOffset;
    this.columnIndexLength = columnIndexLength;
    this.offsetIndexOffset = offsetIndexOffset;
    this.offsetIndexLength = offsetIndexLength;
    this.sizeStatistics = sizeStatistics;
    this.geospatialStatistics = geospatialStatistics;
    this.keyValueMetadata = keyValueMetadata;
  }

  /** Returns the size statistics, or null if unavailable. */
  public SizeStatistics sizeStatistics() {
    return sizeStatistics;
  }

  /** Returns the geospatial statistics, or null if unavailable. */
  public GeospatialStatistics geospatialStatistics() {
    return geospatialStatistics;
  }

  /** Returns the column-level key-value metadata, or null if unavailable. */
  public List<KeyValue> keyValueMetadata() {
    return keyValueMetadata;
  }

  public boolean hasBloomFilter() {
    return bloomFilterOffset >= 0 && bloomFilterLength > 0;
  }

  public boolean hasColumnIndex() {
    return columnIndexOffset >= 0 && columnIndexLength > 0;
  }

  public boolean hasOffsetIndex() {
    return offsetIndexOffset >= 0 && offsetIndexLength > 0;
  }

  public long bloomFilterOffset() {
    return bloomFilterOffset;
  }

  public int bloomFilterLength() {
    return bloomFilterLength;
  }

  public long columnIndexOffset() {
    return columnIndexOffset;
  }

  public int columnIndexLength() {
    return columnIndexLength;
  }

  public long offsetIndexOffset() {
    return offsetIndexOffset;
  }

  public int offsetIndexLength() {
    return offsetIndexLength;
  }
}

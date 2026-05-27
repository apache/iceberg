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

import java.util.Set;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;

public abstract class ColumnChunkMetadata {
  public static ColumnChunkMetadata get(
      ColumnPath path,
      PrimitiveType type,
      CompressionCodecName codec,
      EncodingStats encodingStats,
      Set<Encoding> encodings,
      Statistics<?> statistics,
      long firstDataPage,
      long dictionaryPageOffset,
      long valueCount,
      long totalSize,
      long totalUncompressedSize) {
    if (positiveLongFitsInAnInt(firstDataPage)
        && positiveLongFitsInAnInt(dictionaryPageOffset)
        && positiveLongFitsInAnInt(valueCount)
        && positiveLongFitsInAnInt(totalSize)
        && positiveLongFitsInAnInt(totalUncompressedSize)) {
      return new IntColumnChunkMetadata(
          path,
          type,
          codec,
          encodingStats,
          encodings,
          statistics,
          firstDataPage,
          dictionaryPageOffset,
          valueCount,
          totalSize,
          totalUncompressedSize);
    }
    return new LongColumnChunkMetadata(
        path,
        type,
        codec,
        encodingStats,
        encodings,
        statistics,
        firstDataPage,
        dictionaryPageOffset,
        valueCount,
        totalSize,
        totalUncompressedSize);
  }

  public long getStartingPos() {
    long dictionaryPageOffset = dictionaryPageOffset();
    long firstDataPageOffset = firstDataPageOffset();
    if (dictionaryPageOffset > 0 && dictionaryPageOffset < firstDataPageOffset) {
      return dictionaryPageOffset;
    }
    return firstDataPageOffset;
  }

  protected static boolean positiveLongFitsInAnInt(long value) {
    return (value >= 0) && (value + Integer.MIN_VALUE <= Integer.MAX_VALUE);
  }

  private final EncodingStats encodingStats;
  private final ColumnChunkProperties properties;

  protected ColumnChunkMetadata(
      EncodingStats encodingStats, ColumnChunkProperties columnChunkProperties) {
    this.encodingStats = encodingStats;
    this.properties = columnChunkProperties;
  }

  public CompressionCodecName codec() {
    return properties.codec();
  }

  public ColumnPath path() {
    return properties.path();
  }

  public PrimitiveType.PrimitiveTypeName typeName() {
    return properties.type().getPrimitiveTypeName();
  }

  public PrimitiveType primitiveType() {
    return properties.type();
  }

  public abstract long firstDataPageOffset();

  public abstract long dictionaryPageOffset();

  public abstract long valueCount();

  public abstract long totalUncompressedSize();

  public abstract long totalSize();

  public abstract Statistics<?> statistics();

  public Set<Encoding> encodings() {
    return properties.encodings();
  }

  public EncodingStats encodingStats() {
    return encodingStats;
  }

  @Override
  public String toString() {
    return "ColumnMetaData{" + properties.toString() + ", " + firstDataPageOffset() + "}";
  }
}

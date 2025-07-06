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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

public class ParquetUtil {
  // not meant to be instantiated
  private ParquetUtil() {}

  private static final long UNIX_EPOCH_JULIAN = 2_440_588L;

  public static Metrics fileMetrics(InputFile file, MetricsConfig metricsConfig) {
    return fileMetrics(file, metricsConfig, null);
  }

  public static Metrics fileMetrics(
      InputFile file, MetricsConfig metricsConfig, NameMapping nameMapping) {
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(file))) {
      return footerMetrics(reader.getFooter(), Stream.empty(), metricsConfig, nameMapping);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read footer of file: %s", file.location());
    }
  }

  public static Metrics footerMetrics(
      ParquetMetadata metadata, Stream<FieldMetrics<?>> fieldMetrics, MetricsConfig metricsConfig) {
    return footerMetrics(metadata, fieldMetrics, metricsConfig, null);
  }

  public static Metrics footerMetrics(
      ParquetMetadata metadata,
      Stream<FieldMetrics<?>> fieldMetrics,
      MetricsConfig metricsConfig,
      NameMapping nameMapping) {
    Preconditions.checkNotNull(fieldMetrics, "fieldMetrics should not be null");
    MessageType parquetTypeWithIds = getParquetTypeWithIds(metadata, nameMapping);
    Schema fileSchema = ParquetSchemaUtil.convertAndPrune(parquetTypeWithIds);
    return ParquetMetrics.metrics(
        fileSchema, parquetTypeWithIds, metricsConfig, metadata, fieldMetrics);
  }

  private static MessageType getParquetTypeWithIds(
      ParquetMetadata metadata, NameMapping nameMapping) {
    MessageType type = metadata.getFileMetaData().getSchema();

    if (ParquetSchemaUtil.hasIds(type)) {
      return type;
    }

    if (nameMapping != null) {
      return ParquetSchemaUtil.applyNameMapping(type, nameMapping);
    }

    return ParquetSchemaUtil.addFallbackIds(type);
  }

  /**
   * Returns a list of offsets in ascending order determined by the starting position of the row
   * groups.
   */
  public static List<Long> getSplitOffsets(ParquetMetadata md) {
    List<Long> splitOffsets = Lists.newArrayListWithExpectedSize(md.getBlocks().size());
    for (BlockMetaData blockMetaData : md.getBlocks()) {
      splitOffsets.add(blockMetaData.getStartingPos());
    }
    Collections.sort(splitOffsets);
    return splitOffsets;
  }

  @SuppressWarnings("deprecation")
  public static boolean hasNonDictionaryPages(ColumnChunkMetaData meta) {
    EncodingStats stats = meta.getEncodingStats();
    if (stats != null) {
      return stats.hasNonDictionaryEncodedPages();
    }

    // without EncodingStats, fall back to testing the encoding list
    Set<Encoding> encodings = Sets.newHashSet(meta.getEncodings());
    if (encodings.remove(Encoding.PLAIN_DICTIONARY)) {
      // if remove returned true, PLAIN_DICTIONARY was present, which means at
      // least one page was dictionary encoded and 1.0 encodings are used

      // RLE and BIT_PACKED are only used for repetition or definition levels
      encodings.remove(Encoding.RLE);
      encodings.remove(Encoding.BIT_PACKED);

      // when empty, no encodings other than dictionary or rep/def levels
      return !encodings.isEmpty();
    } else {
      // if PLAIN_DICTIONARY wasn't present, then either the column is not
      // dictionary-encoded, or the 2.0 encoding, RLE_DICTIONARY, was used.
      // for 2.0, this cannot determine whether a page fell back without
      // page encoding stats
      return true;
    }
  }

  public static boolean hasNoBloomFilterPages(ColumnChunkMetaData meta) {
    return meta.getBloomFilterOffset() <= 0;
  }

  public static Dictionary readDictionary(ColumnDescriptor desc, PageReader pageSource) {
    DictionaryPage dictionaryPage = pageSource.readDictionaryPage();
    if (dictionaryPage != null) {
      try {
        return dictionaryPage.getEncoding().initDictionary(desc, dictionaryPage);
      } catch (IOException e) {
        throw new ParquetDecodingException("could not decode the dictionary for " + desc, e);
      }
    }
    return null;
  }

  public static boolean isIntType(PrimitiveType primitiveType) {
    if (primitiveType.getOriginalType() != null) {
      switch (primitiveType.getOriginalType()) {
        case INT_8:
        case INT_16:
        case INT_32:
        case DATE:
          return true;
        default:
          return false;
      }
    }
    return primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32;
  }

  /**
   * Method to read timestamp (parquet Int96) from bytebuffer. Read 12 bytes in byteBuffer: 8 bytes
   * (time of day nanos) + 4 bytes(julianDay)
   */
  public static long extractTimestampInt96(ByteBuffer buffer) {
    // 8 bytes (time of day nanos)
    long timeOfDayNanos = buffer.getLong();
    // 4 bytes(julianDay)
    int julianDay = buffer.getInt();
    return TimeUnit.DAYS.toMicros(julianDay - UNIX_EPOCH_JULIAN)
        + TimeUnit.NANOSECONDS.toMicros(timeOfDayNanos);
  }
}

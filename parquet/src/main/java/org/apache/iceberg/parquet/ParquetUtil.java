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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.MetricsModes.MetricsMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import static org.apache.iceberg.parquet.ParquetConversions.fromParquetPrimitive;
import static org.apache.iceberg.util.BinaryUtil.truncateBinaryMax;
import static org.apache.iceberg.util.BinaryUtil.truncateBinaryMin;
import static org.apache.iceberg.util.UnicodeUtil.truncateStringMax;
import static org.apache.iceberg.util.UnicodeUtil.truncateStringMin;

public class ParquetUtil {
  // not meant to be instantiated
  private ParquetUtil() {
  }

  // Access modifier is package-private, to only allow use from existing tests
  static Metrics fileMetrics(InputFile file) {
    return fileMetrics(file, MetricsConfig.getDefault());
  }

  public static Metrics fileMetrics(InputFile file, MetricsConfig metricsConfig) {
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(file))) {
      return footerMetrics(reader.getFooter(), metricsConfig);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read footer of file: %s", file);
    }
  }

  public static Metrics footerMetrics(ParquetMetadata metadata, MetricsConfig metricsConfig) {
    long rowCount = 0;
    Map<Integer, Long> columnSizes = Maps.newHashMap();
    Map<Integer, Long> valueCounts = Maps.newHashMap();
    Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    Map<Integer, Literal<?>> lowerBounds = Maps.newHashMap();
    Map<Integer, Literal<?>> upperBounds = Maps.newHashMap();
    Set<Integer> missingStats = Sets.newHashSet();

    MessageType parquetType = metadata.getFileMetaData().getSchema();
    Schema fileSchema = ParquetSchemaUtil.convert(parquetType);

    List<BlockMetaData> blocks = metadata.getBlocks();
    for (BlockMetaData block : blocks) {
      rowCount += block.getRowCount();
      for (ColumnChunkMetaData column : block.getColumns()) {
        ColumnPath path = column.getPath();
        int fieldId = fileSchema.aliasToId(path.toDotString());
        increment(columnSizes, fieldId, column.getTotalSize());

        String columnName = fileSchema.findColumnName(fieldId);
        MetricsMode metricsMode = metricsConfig.columnMode(columnName);
        if (metricsMode == MetricsModes.None.get()) {
          continue;
        }
        increment(valueCounts, fieldId, column.getValueCount());

        Statistics stats = column.getStatistics();
        if (stats == null) {
          missingStats.add(fieldId);
        } else if (!stats.isEmpty()) {
          increment(nullValueCounts, fieldId, stats.getNumNulls());

          if (metricsMode != MetricsModes.Counts.get()) {
            Types.NestedField field = fileSchema.findField(fieldId);
            if (field != null && stats.hasNonNullValue() && shouldStoreBounds(path, fileSchema)) {
              Literal<?> min = fromParquetPrimitive(field.type(), column.getPrimitiveType(), stats.genericGetMin());
              updateMin(lowerBounds, fieldId, field.type(), min, metricsMode);
              Literal<?> max = fromParquetPrimitive(field.type(), column.getPrimitiveType(), stats.genericGetMax());
              updateMax(upperBounds, fieldId, field.type(), max, metricsMode);
            }
          }
        }
      }
    }

    // discard accumulated values if any stats were missing
    for (Integer fieldId : missingStats) {
      nullValueCounts.remove(fieldId);
      lowerBounds.remove(fieldId);
      upperBounds.remove(fieldId);
    }

    return new Metrics(rowCount, columnSizes, valueCounts, nullValueCounts,
        toBufferMap(fileSchema, lowerBounds), toBufferMap(fileSchema, upperBounds));
  }


  /**
   * @return a list of offsets in ascending order determined by the starting position
   * of the row groups
   */
  public static List<Long> getSplitOffsets(ParquetMetadata md) {
    List<Long> splitOffsets = new ArrayList<>(md.getBlocks().size());
    for (BlockMetaData blockMetaData : md.getBlocks()) {
      splitOffsets.add(blockMetaData.getStartingPos());
    }
    Collections.sort(splitOffsets);
    return ImmutableList.copyOf(splitOffsets);
  }

  // we allow struct nesting, but not maps or arrays
  private static boolean shouldStoreBounds(ColumnPath columnPath, Schema schema) {
    Iterator<String> pathIterator = columnPath.iterator();
    Type currentType = schema.asStruct();

    while (pathIterator.hasNext()) {
      if (currentType == null || !currentType.isStructType()) {
        return false;
      }
      String fieldName = pathIterator.next();
      currentType = currentType.asStructType().fieldType(fieldName);
    }

    return currentType != null && currentType.isPrimitiveType();
  }

  private static void increment(Map<Integer, Long> columns, int fieldId, long amount) {
    if (columns != null) {
      if (columns.containsKey(fieldId)) {
        columns.put(fieldId, columns.get(fieldId) + amount);
      } else {
        columns.put(fieldId, amount);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> void updateMin(Map<Integer, Literal<?>> lowerBounds, int id, Type type,
                                    Literal<T> min, MetricsMode metricsMode) {
    Literal<T> currentMin = (Literal<T>) lowerBounds.get(id);
    if (currentMin == null || min.comparator().compare(min.value(), currentMin.value()) < 0) {
      if (metricsMode == MetricsModes.Full.get()) {
        lowerBounds.put(id, min);
      } else {
        MetricsModes.Truncate truncateMode = (MetricsModes.Truncate) metricsMode;
        int truncateLength = truncateMode.length();
        switch (type.typeId()) {
          case STRING:
            lowerBounds.put(id, truncateStringMin((Literal<CharSequence>) min, truncateLength));
            break;
          case FIXED:
          case BINARY:
            lowerBounds.put(id, truncateBinaryMin((Literal<ByteBuffer>) min, truncateLength));
            break;
          default:
            lowerBounds.put(id, min);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> void updateMax(Map<Integer, Literal<?>> upperBounds, int id, Type type,
                                    Literal<T> max, MetricsMode metricsMode) {
    Literal<T> currentMax = (Literal<T>) upperBounds.get(id);
    if (currentMax == null || max.comparator().compare(max.value(), currentMax.value()) > 0) {
      if (metricsMode == MetricsModes.Full.get()) {
        upperBounds.put(id, max);
      } else {
        MetricsModes.Truncate truncateMode = (MetricsModes.Truncate) metricsMode;
        int truncateLength = truncateMode.length();
        switch (type.typeId()) {
          case STRING:
            upperBounds.put(id, truncateStringMax((Literal<CharSequence>) max, truncateLength));
            break;
          case FIXED:
          case BINARY:
            upperBounds.put(id, truncateBinaryMax((Literal<ByteBuffer>) max, truncateLength));
            break;
          default:
            upperBounds.put(id, max);
        }
      }
    }
  }

  private static Map<Integer, ByteBuffer> toBufferMap(Schema schema, Map<Integer, Literal<?>> map) {
    Map<Integer, ByteBuffer> bufferMap = Maps.newHashMap();
    for (Map.Entry<Integer, Literal<?>> entry : map.entrySet()) {
      bufferMap.put(entry.getKey(),
          Conversions.toByteBuffer(schema.findType(entry.getKey()), entry.getValue().value()));
    }
    return bufferMap;
  }
}

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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iceberg.parquet.ParquetConversions.fromParquetPrimitive;

public class ParquetMetrics implements Serializable {
  private ParquetMetrics() {
  }

  public static Metrics fromInputFile(InputFile file) {
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(file))) {
      return fromMetadata(reader.getFooter());
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read footer of file: %s", file);
    }
  }

  public static Metrics fromMetadata(ParquetMetadata metadata) {
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
        int fieldId = fileSchema.aliasToId(column.getPath().toDotString());
        increment(columnSizes, fieldId, column.getTotalSize());
        increment(valueCounts, fieldId, column.getValueCount());

        Statistics stats = column.getStatistics();
        if (stats == null) {
          missingStats.add(fieldId);
        } else if (!stats.isEmpty()) {
          increment(nullValueCounts, fieldId, stats.getNumNulls());

          // only add min/max stats for top-level fields
          // TODO: allow struct nesting, but not maps or arrays
          Types.NestedField field = fileSchema.asStruct().field(fieldId);
          if (field != null && stats.hasNonNullValue()) {
            updateMin(lowerBounds, fieldId,
                fromParquetPrimitive(field.type(), stats.genericGetMin()));
            updateMax(upperBounds, fieldId,
                fromParquetPrimitive(field.type(), stats.genericGetMax()));
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
  private static <T> void updateMin(Map<Integer, Literal<?>> lowerBounds, int id, Literal<T> min) {
    Literal<T> currentMin = (Literal<T>) lowerBounds.get(id);
    if (currentMin == null || min.comparator().compare(min.value(), currentMin.value()) < 0) {
      lowerBounds.put(id, min);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> void updateMax(Map<Integer, Literal<?>> upperBounds, int id, Literal<T> max) {
    Literal<T> currentMax = (Literal<T>) upperBounds.get(id);
    if (currentMax == null || max.comparator().compare(max.value(), currentMax.value()) > 0) {
      upperBounds.put(id, max);
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

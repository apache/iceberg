/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.parquet;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.iceberg.Metrics;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.InputFile;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        }
      }
    }

    // discard accumulated values if any stats were missing
    for (Integer fieldId : missingStats) {
      nullValueCounts.remove(fieldId);
    }

    return new Metrics(
        rowCount, columnSizes, valueCounts, nullValueCounts, null);
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
}

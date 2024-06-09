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
package org.apache.iceberg.flink.source.reader;

import java.io.Serializable;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;

/**
 * {@link SplitWatermarkExtractor} implementation which uses an Iceberg timestamp column statistics
 * to get the watermarks for the {@link IcebergSourceSplit}. This watermark is emitted by the {@link
 * WatermarkExtractorRecordEmitter} along with the actual records.
 */
@Internal
public class ColumnStatsWatermarkExtractor implements SplitWatermarkExtractor, Serializable {
  private final int eventTimeFieldId;
  private final String eventTimeFieldName;
  private final TimeUnit timeUnit;

  /**
   * Creates the extractor.
   *
   * @param schema The schema of the Table
   * @param eventTimeFieldName The column which should be used as an event time
   * @param timeUnit Used for converting the long value to epoch milliseconds
   */
  public ColumnStatsWatermarkExtractor(
      Schema schema, String eventTimeFieldName, TimeUnit timeUnit) {
    Types.NestedField field = schema.findField(eventTimeFieldName);
    TypeID typeID = field.type().typeId();
    Preconditions.checkArgument(
        typeID.equals(TypeID.LONG) || typeID.equals(TypeID.TIMESTAMP),
        "Found %s, expected a LONG or TIMESTAMP column for watermark generation.",
        typeID);
    this.eventTimeFieldId = field.fieldId();
    this.eventTimeFieldName = eventTimeFieldName;
    // Use the timeUnit only for Long columns.
    this.timeUnit = typeID.equals(TypeID.LONG) ? timeUnit : TimeUnit.MICROSECONDS;
  }

  @VisibleForTesting
  ColumnStatsWatermarkExtractor(int eventTimeFieldId, String eventTimeFieldName) {
    this.eventTimeFieldId = eventTimeFieldId;
    this.eventTimeFieldName = eventTimeFieldName;
    this.timeUnit = TimeUnit.MICROSECONDS;
  }

  /**
   * Get the watermark for a split using column statistics.
   *
   * @param split The split
   * @return The watermark
   * @throws IllegalArgumentException if there is no statistics for the column
   */
  @Override
  public long extractWatermark(IcebergSourceSplit split) {
    return split.task().files().stream()
        .map(
            scanTask -> {
              Preconditions.checkArgument(
                  scanTask.file().lowerBounds() != null
                      && scanTask.file().lowerBounds().get(eventTimeFieldId) != null,
                  "Missing statistics for column name = %s in file = %s",
                  eventTimeFieldName,
                  eventTimeFieldId,
                  scanTask.file());
              return timeUnit.toMillis(
                  Conversions.fromByteBuffer(
                      Types.LongType.get(), scanTask.file().lowerBounds().get(eventTimeFieldId)));
            })
        .min(Comparator.comparingLong(l -> l))
        .get();
  }
}

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
package org.apache.iceberg.flink.source.eventtimeextractor;

import java.io.Serializable;
import java.util.Comparator;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * {@link IcebergEventTimeExtractor} implementation which uses an Iceberg timestamp column to get
 * the watermarks and the event times for the {@link RowData} read by the reader function.
 */
public class IcebergTimestampEventTimeExtractor
    implements IcebergEventTimeExtractor<RowData>, Serializable {
  private final int tsFieldId;
  private final int tsFiledPos;

  /**
   * Creates the extractor.
   *
   * @param schema The schema of the Table
   * @param tsFieldName The timestamp column which should be used as an event time
   */
  public IcebergTimestampEventTimeExtractor(Schema schema, String tsFieldName) {
    Types.NestedField field = schema.findField(tsFieldName);
    Preconditions.checkArgument(
        field.type().typeId().equals(Type.TypeID.TIMESTAMP), "Type should be timestamp");
    this.tsFieldId = field.fieldId();
    this.tsFiledPos = FlinkSchemaUtil.convert(schema).getFieldIndex(tsFieldName);
  }

  @Override
  public long extractWatermark(IcebergSourceSplit split) {
    return split.task().files().stream()
        .map(
            scanTask ->
                (long)
                        Conversions.fromByteBuffer(
                            Types.LongType.get(), scanTask.file().lowerBounds().get(tsFieldId))
                    / 1000L)
        .min(Comparator.comparingLong(l -> l))
        .get();
  }

  @Override
  public long extractEventTime(RowData rowData) {
    return rowData.getTimestamp(tsFiledPos, 0).getMillisecond();
  }
}

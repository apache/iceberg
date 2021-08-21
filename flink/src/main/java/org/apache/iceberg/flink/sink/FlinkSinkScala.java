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

package org.apache.iceberg.flink.sink;

import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import scala.collection.JavaConverters;

public class FlinkSinkScala {

  private FlinkSinkScala() {
  }

  /**
   * Initialize a {@link FlinkSink.Builder} to export the data from generic input data stream into iceberg table. We use
   * {@link RowData} inside the sink connector, so users need to provide a mapper function and a
   * {@link TypeInformation} to convert those generic records to a RowData DataStream.
   *
   * @param input      the generic source input data stream.
   * @param mapper     function to convert the generic data to {@link RowData}
   * @param outputType to define the {@link TypeInformation} for the input data.
   * @param <T>        the data type of records.
   * @return {@link FlinkSink.Builder} to connect the iceberg table.
   */
  public static <T> Builder builderFor(
      DataStream<T> input,
      MapFunction<T, RowData> mapper,
      TypeInformation<RowData> outputType) {
    return new Builder().forMapperOutputType(input.javaStream(), mapper, outputType);
  }

  /**
   * Initialize a {@link Builder} to export the data from input data stream with {@link Row}s into iceberg table. We use
   * {@link RowData} inside the sink connector, so users need to provide a {@link TableSchema} for builder to convert
   * those {@link Row}s to a {@link RowData} DataStream.
   *
   * @param input       the source input data stream with {@link Row}s.
   * @param tableSchema defines the {@link TypeInformation} for input data.
   * @return {@link Builder} to connect the iceberg table.
   */
  public static Builder forRow(DataStream<Row> input, TableSchema tableSchema) {
    RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
    DataType[] fieldDataTypes = tableSchema.getFieldDataTypes();

    DataFormatConverters.RowConverter rowConverter = new DataFormatConverters.RowConverter(fieldDataTypes);
    return builderFor(input, rowConverter::toInternal, FlinkCompatibilityUtil.toTypeInfo(rowType))
        .tableSchema(tableSchema);
  }

  /**
   * Initialize a {@link Builder} to export the data from input data stream with {@link RowData}s into iceberg table.
   *
   * @param input the source input data stream with {@link RowData}s.
   * @return {@link Builder} to connect the iceberg table.
   */
  public static Builder forRowData(DataStream<RowData> input) {
    return new Builder().forRowData(input.javaStream());
  }

  public static class Builder extends FlinkSink.Builder<Builder> {
    /**
     * Configuring the equality field columns for iceberg table that accept CDC or UPSERT events.
     *
     * @param columnsScala defines the iceberg table's key.
     * @return {@link FlinkSink.Builder} to connect the iceberg table.
     */
    public Builder equalityFieldColumns(scala.collection.immutable.List<String> columnsScala) {
      super.equalityFieldColumns((List<String>) JavaConverters.seqAsJavaList(columnsScala));
      return this;
    }
  }
}

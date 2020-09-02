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

package org.apache.iceberg.flink;

import java.util.Arrays;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.sink.FlinkSink;

public class IcebergTableSink implements AppendStreamTableSink<RowData> {
  private final TableLoader tableLoader;
  private final TableSchema tableSchema;
  private final Configuration hadoopConf;

  public IcebergTableSink(TableLoader tableLoader, Configuration hadoopConf, TableSchema tableSchema) {
    this.tableLoader = tableLoader;
    this.hadoopConf = hadoopConf;
    this.tableSchema = tableSchema;
  }

  @Override
  public DataStreamSink<?> consumeDataStream(DataStream<RowData> dataStream) {
    return FlinkSink.forRowData(dataStream)
        .tableLoader(tableLoader)
        .hadoopConf(hadoopConf)
        .tableSchema(tableSchema)
        .build();
  }

  @Override
  public DataType getConsumedDataType() {
    return tableSchema.toRowDataType().bridgedTo(RowData.class);
  }

  @Override
  public TableSchema getTableSchema() {
    return this.tableSchema;
  }

  @Override
  public TableSink<RowData> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
    if (!Arrays.equals(tableSchema.getFieldNames(), fieldNames)) {
      String expectedFieldNames = Arrays.toString(tableSchema.getFieldNames());
      String actualFieldNames = Arrays.toString(fieldNames);
      throw new ValidationException("The field names is mismatched. Expected: " +
          expectedFieldNames + " But was: " + actualFieldNames);
    }
    if (!Arrays.equals(tableSchema.getFieldTypes(), fieldTypes)) {
      String expectedFieldTypes = Arrays.toString(tableSchema.getFieldTypes());
      String actualFieldTypes = Arrays.toString(fieldNames);
      throw new ValidationException("Field types are mismatched. Expected: " +
          expectedFieldTypes + " But was: " + actualFieldTypes);
    }
    return this;
  }
}

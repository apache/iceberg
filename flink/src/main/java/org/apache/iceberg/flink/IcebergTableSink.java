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

import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.OverwritableTableSink;
import org.apache.flink.table.sinks.PartitionableTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.flink.sink.FlinkSink;

public class IcebergTableSink implements AppendStreamTableSink<RowData>, OverwritableTableSink, PartitionableTableSink {
  private final boolean isBounded;
  private final TableLoader tableLoader;
  private final TableSchema tableSchema;

  private boolean overwrite = false;

  public IcebergTableSink(boolean isBounded, TableLoader tableLoader, TableSchema tableSchema) {
    this.isBounded = isBounded;
    this.tableLoader = tableLoader;
    this.tableSchema = tableSchema;
  }

  @Override
  public DataStreamSink<?> consumeDataStream(DataStream<RowData> dataStream) {
    Preconditions.checkState(!overwrite || isBounded, "Unbounded data stream doesn't support overwrite operation.");

    return FlinkSink.forRowData(dataStream)
        .tableLoader(tableLoader)
        .tableSchema(tableSchema)
        .overwrite(overwrite)
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
    // This method has been deprecated and it will be removed in future version, so left the empty implementation here.
    return this;
  }

  @Override
  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  @Override
  public void setStaticPartition(Map<String, String> partitions) {
    // The flink's PartitionFanoutWriter will handle the static partition write policy automatically.
  }
}

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
import java.util.Map;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;

public interface BaseIcebergSinkBuilder {

  BaseIcebergSinkBuilder tableSchema(TableSchema newTableSchema);

  BaseIcebergSinkBuilder tableLoader(TableLoader newTableLoader);

  BaseIcebergSinkBuilder equalityFieldColumns(List<String> columns);

  BaseIcebergSinkBuilder overwrite(boolean newOverwrite);

  BaseIcebergSinkBuilder setAll(Map<String, String> properties);

  BaseIcebergSinkBuilder flinkConf(ReadableConfig config);

  BaseIcebergSinkBuilder table(Table newTable);

  BaseIcebergSinkBuilder writeParallelism(int newWriteParallelism);

  BaseIcebergSinkBuilder distributionMode(DistributionMode mode);

  BaseIcebergSinkBuilder toBranch(String branch);

  BaseIcebergSinkBuilder upsert(boolean enabled);

  DataStreamSink<?> append();

  @VisibleForTesting
  List<Integer> checkAndGetEqualityFieldIds();

  static BaseIcebergSinkBuilder forRow(
      DataStream<Row> input, TableSchema tableSchema, boolean useV2Sink) {
    if (useV2Sink) {
      return IcebergSink.forRow(input, tableSchema);
    } else {
      return FlinkSink.forRow(input, tableSchema);
    }
  }

  static BaseIcebergSinkBuilder forRowData(DataStream<RowData> input, boolean useV2Sink) {
    if (useV2Sink) {
      return IcebergSink.forRowData(input);
    } else {
      return FlinkSink.forRowData(input);
    }
  }
}

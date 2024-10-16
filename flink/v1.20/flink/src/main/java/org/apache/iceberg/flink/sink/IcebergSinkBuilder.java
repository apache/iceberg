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
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;

@Internal
/*
 This class is for internal purpose of transition between the previous implementation of Flink's sink (FlinkSink)
 and the new one implementation based on Flink v2 sink's API (IcebergSink). After we remove the previous implementation,
 all occurrences of this class would be replaced by direct IcebergSink usage.
*/
public interface IcebergSinkBuilder<T extends IcebergSinkBuilder<?>> {

  T tableSchema(TableSchema newTableSchema);

  T tableLoader(TableLoader newTableLoader);

  T equalityFieldColumns(List<String> columns);

  T overwrite(boolean newOverwrite);

  T setAll(Map<String, String> properties);

  T flinkConf(ReadableConfig config);

  T table(Table newTable);

  T writeParallelism(int newWriteParallelism);

  T distributionMode(DistributionMode mode);

  T toBranch(String branch);

  T upsert(boolean enabled);

  DataStreamSink<?> append();

  static IcebergSinkBuilder<?> forRow(
      DataStream<Row> input, TableSchema tableSchema, boolean useV2Sink) {
    if (useV2Sink) {
      return IcebergSink.forRow(input, tableSchema);
    } else {
      return FlinkSink.forRow(input, tableSchema);
    }
  }

  static IcebergSinkBuilder<?> forRowData(DataStream<RowData> input, boolean useV2Sink) {
    if (useV2Sink) {
      return IcebergSink.forRowData(input);
    } else {
      return FlinkSink.forRowData(input);
    }
  }
}

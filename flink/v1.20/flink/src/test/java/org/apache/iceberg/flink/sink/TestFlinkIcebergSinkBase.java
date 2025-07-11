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

import static org.apache.iceberg.flink.TestFixtures.DATABASE;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestFlinkIcebergSinkBase {

  @RegisterExtension
  public static MiniClusterExtension miniClusterResource =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  protected static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  protected static final TypeInformation<Row> ROW_TYPE_INFO =
      new RowTypeInfo(
          SimpleDataUtil.FLINK_SCHEMA.getColumnDataTypes().stream()
              .map(ExternalTypeInfo::of)
              .toArray(TypeInformation[]::new));

  protected static final DataFormatConverters.RowConverter CONVERTER =
      new DataFormatConverters.RowConverter(
          SimpleDataUtil.FLINK_SCHEMA.getColumnDataTypes().toArray(DataType[]::new));

  protected TableLoader tableLoader;
  protected Table table;
  protected StreamExecutionEnvironment env;

  protected <T> BoundedTestSource<T> createBoundedSource(List<T> rows) {
    return new BoundedTestSource<>(Collections.singletonList(rows));
  }

  protected List<Row> createRows(String prefix) {
    return Lists.newArrayList(
        Row.of(1, prefix + "aaa"),
        Row.of(1, prefix + "bbb"),
        Row.of(1, prefix + "ccc"),
        Row.of(2, prefix + "aaa"),
        Row.of(2, prefix + "bbb"),
        Row.of(2, prefix + "ccc"),
        Row.of(3, prefix + "aaa"),
        Row.of(3, prefix + "bbb"),
        Row.of(3, prefix + "ccc"));
  }

  protected List<RowData> convertToRowData(List<Row> rows) {
    return rows.stream().map(CONVERTER::toInternal).collect(Collectors.toList());
  }

  protected void testWriteRow(
      int writerParallelism,
      ResolvedSchema resolvedSchema,
      DistributionMode distributionMode,
      boolean isTableSchema)
      throws Exception {
    List<Row> rows = createRows("");
    DataStream<Row> dataStream = env.addSource(createBoundedSource(rows), ROW_TYPE_INFO);

    if (isTableSchema) {
      FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_TABLE_SCHEMA)
          .table(table)
          .tableLoader(tableLoader)
          .tableSchema(
              resolvedSchema != null ? TableSchema.fromResolvedSchema(resolvedSchema) : null)
          .writeParallelism(writerParallelism)
          .distributionMode(distributionMode)
          .append();
    } else {
      FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
          .table(table)
          .tableLoader(tableLoader)
          .resolvedSchema(resolvedSchema)
          .writeParallelism(writerParallelism)
          .distributionMode(distributionMode)
          .append();
    }

    // Execute the program.
    env.execute("Test Iceberg DataStream.");

    SimpleDataUtil.assertTableRows(table, convertToRowData(rows));
  }

  protected int partitionFiles(String partition) throws IOException {
    return SimpleDataUtil.partitionDataFiles(table, ImmutableMap.of("data", partition)).size();
  }
}

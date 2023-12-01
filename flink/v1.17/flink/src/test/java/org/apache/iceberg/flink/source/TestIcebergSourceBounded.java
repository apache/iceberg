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
package org.apache.iceberg.flink.source;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.data.RowDataToRowMapper;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class TestIcebergSourceBounded extends TestFlinkScan {
  @Override
  protected List<Row> runWithProjection(String... projected) throws Exception {
    Schema icebergTableSchema =
        catalogResource.catalog().loadTable(TestFixtures.TABLE_IDENTIFIER).schema();
    TableSchema.Builder builder = TableSchema.builder();
    TableSchema schema = FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(icebergTableSchema));
    for (String field : projected) {
      TableColumn column = schema.getTableColumn(field).get();
      builder.field(column.getName(), column.getType());
    }
    TableSchema flinkSchema = builder.build();
    Schema projectedSchema = FlinkSchemaUtil.convert(icebergTableSchema, flinkSchema);
    return run(projectedSchema, Lists.newArrayList(), Maps.newHashMap(), "", projected);
  }

  @Override
  protected List<Row> runWithFilter(Expression filter, String sqlFilter, boolean caseSensitive)
      throws Exception {
    Map<String, String> options = Maps.newHashMap();
    options.put("case-sensitive", Boolean.toString(caseSensitive));
    return run(null, Collections.singletonList(filter), options, sqlFilter, "*");
  }

  @Override
  protected List<Row> runWithOptions(Map<String, String> options) throws Exception {
    return run(null, Lists.newArrayList(), options, "", "*");
  }

  @Override
  protected List<Row> run() throws Exception {
    return run(null, Lists.newArrayList(), Maps.newHashMap(), "", "*");
  }

  protected List<Row> run(
      Schema projectedSchema,
      List<Expression> filters,
      Map<String, String> options,
      String sqlFilter,
      String... sqlSelectedFields)
      throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    Configuration config = new Configuration();
    config.setInteger(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 128);
    Table table;
    try (TableLoader tableLoader = tableLoader()) {
      tableLoader.open();
      table = tableLoader.loadTable();
    }

    IcebergSource.Builder<RowData> sourceBuilder =
        IcebergSource.forRowData()
            .tableLoader(tableLoader())
            .table(table)
            .assignerFactory(new SimpleSplitAssignerFactory())
            .flinkConfig(config);
    if (projectedSchema != null) {
      sourceBuilder.project(projectedSchema);
    }

    sourceBuilder.filters(filters);
    sourceBuilder.properties(options);

    DataStream<Row> stream =
        env.fromSource(
                sourceBuilder.build(),
                WatermarkStrategy.noWatermarks(),
                "testBasicRead",
                TypeInformation.of(RowData.class))
            .map(
                new RowDataToRowMapper(
                    FlinkSchemaUtil.convert(
                        projectedSchema == null ? table.schema() : projectedSchema)));

    try (CloseableIterator<Row> iter = stream.executeAndCollect()) {
      return Lists.newArrayList(iter);
    }
  }
}

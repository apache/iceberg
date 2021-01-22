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
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.sources.FilterableTableSource;
import org.apache.flink.table.sources.LimitableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Flink Iceberg table source.
 */
public class IcebergTableSource
    implements StreamTableSource<RowData>, ProjectableTableSource<RowData>, FilterableTableSource<RowData>,
    LimitableTableSource<RowData> {

  private static final Joiner COMMA = Joiner.on(',');

  private final TableLoader loader;
  private final TableSchema schema;
  private final Map<String, String> properties;
  private final int[] projectedFields;
  private final boolean isLimitPushDown;
  private final long limit;
  private final List<org.apache.iceberg.expressions.Expression> filters;
  private final ReadableConfig readableConfig;

  public IcebergTableSource(TableLoader loader, TableSchema schema, Map<String, String> properties,
                            ReadableConfig readableConfig) {
    this(loader, schema, properties, null, false, -1, ImmutableList.of(), readableConfig);
  }

  private IcebergTableSource(TableLoader loader, TableSchema schema, Map<String, String> properties,
                             int[] projectedFields, boolean isLimitPushDown,
                             long limit, List<org.apache.iceberg.expressions.Expression> filters,
                             ReadableConfig readableConfig) {
    this.loader = loader;
    this.schema = schema;
    this.properties = properties;
    this.projectedFields = projectedFields;
    this.isLimitPushDown = isLimitPushDown;
    this.limit = limit;
    this.filters = filters;
    this.readableConfig = readableConfig;
  }

  @Override
  public boolean isBounded() {
    return FlinkSource.isBounded(properties);
  }

  @Override
  public TableSource<RowData> projectFields(int[] fields) {
    return new IcebergTableSource(loader, schema, properties, fields, isLimitPushDown, limit, filters, readableConfig);
  }

  @Override
  public DataStream<RowData> getDataStream(StreamExecutionEnvironment execEnv) {
    return FlinkSource.forRowData()
        .env(execEnv)
        .tableLoader(loader)
        .properties(properties)
        .project(getProjectedSchema())
        .limit(limit)
        .filters(filters)
        .flinkConf(readableConfig)
        .build();
  }

  @Override
  public TableSchema getTableSchema() {
    return schema;
  }

  @Override
  public DataType getProducedDataType() {
    return getProjectedSchema().toRowDataType().bridgedTo(RowData.class);
  }

  private TableSchema getProjectedSchema() {
    TableSchema fullSchema = getTableSchema();
    if (projectedFields == null) {
      return fullSchema;
    } else {
      String[] fullNames = fullSchema.getFieldNames();
      DataType[] fullTypes = fullSchema.getFieldDataTypes();
      return TableSchema.builder().fields(
          Arrays.stream(projectedFields).mapToObj(i -> fullNames[i]).toArray(String[]::new),
          Arrays.stream(projectedFields).mapToObj(i -> fullTypes[i]).toArray(DataType[]::new)).build();
    }
  }

  @Override
  public String explainSource() {
    String explain = "Iceberg table: " + loader.toString();
    if (projectedFields != null) {
      explain += ", ProjectedFields: " + Arrays.toString(projectedFields);
    }

    if (isLimitPushDown) {
      explain += String.format(", LimitPushDown : %d", limit);
    }

    if (isFilterPushedDown()) {
      explain += String.format(", FilterPushDown: %s", COMMA.join(filters));
    }

    return TableConnectorUtils.generateRuntimeName(getClass(), getTableSchema().getFieldNames()) + explain;
  }

  @Override
  public boolean isLimitPushedDown() {
    return isLimitPushDown;
  }

  @Override
  public TableSource<RowData> applyLimit(long newLimit) {
    return new IcebergTableSource(loader, schema, properties, projectedFields, true, newLimit, filters, readableConfig);
  }

  @Override
  public TableSource<RowData> applyPredicate(List<Expression> predicates) {
    List<org.apache.iceberg.expressions.Expression> expressions = Lists.newArrayList();
    for (Expression predicate : predicates) {
      FlinkFilters.convert(predicate).ifPresent(expressions::add);
    }

    return new IcebergTableSource(loader, schema, properties, projectedFields, isLimitPushDown, limit, expressions,
        readableConfig);
  }

  @Override
  public boolean isFilterPushedDown() {
    return this.filters != null && this.filters.size() > 0;
  }
}

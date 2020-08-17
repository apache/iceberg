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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.sources.FilterableTableSource;
import org.apache.flink.table.sources.LimitableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;

/**
 * Flink Iceberg table source.
 * TODO: Implement {@link FilterableTableSource} and {@link LimitableTableSource}.
 */
public class FlinkTableSource implements StreamTableSource<RowData>, ProjectableTableSource<RowData> {

  private final TableIdentifier identifier;
  private final Table table;
  private final CatalogLoader catalogLoader;
  private final Configuration hadoopConf;
  private final TableSchema schema;
  private final Map<String, String> options;
  private final int[] projectedFields;

  public FlinkTableSource(
      TableIdentifier identifier, Table table, CatalogLoader catalogLoader, Configuration hadoopConf,
      TableSchema schema, Map<String, String> options) {
    this(identifier, table, catalogLoader, hadoopConf, schema, options, null);
  }

  private FlinkTableSource(
      TableIdentifier identifier, Table table, CatalogLoader catalogLoader, Configuration hadoopConf,
      TableSchema schema, Map<String, String> options, int[] projectedFields) {
    this.identifier = identifier;
    this.table = table;
    this.catalogLoader = catalogLoader;
    this.hadoopConf = hadoopConf;
    this.schema = schema;
    this.options = options;
    this.projectedFields = projectedFields;
  }

  @Override
  public boolean isBounded() {
    return true;
  }

  @Override
  public TableSource<RowData> projectFields(int[] fields) {
    return new FlinkTableSource(identifier, table, catalogLoader, hadoopConf, schema, options, fields);
  }

  @Override
  public DataStream<RowData> getDataStream(StreamExecutionEnvironment execEnv) {
    Schema icebergSchema = table.schema();
    List<String> projectNames = null;
    if (projectedFields != null) {
      projectNames = Arrays.stream(projectedFields)
                           .mapToObj(project -> icebergSchema.asStruct().fields().get(project).name())
                           .collect(Collectors.toList());
    }
    FlinkInputFormat inputFormat = FlinkInputFormat.builder().table(table)
        .tableLoader(TableLoader.fromCatalog(catalogLoader, identifier)).hadoopConf(hadoopConf)
        .select(projectNames).options(ScanOptions.of(options)).build();
    return execEnv.createInput(inputFormat, RowDataTypeInfo.of((RowType) getProducedDataType().getLogicalType()));
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
    String explain = "Iceberg table: " + identifier;
    if (projectedFields != null) {
      explain += ", ProjectedFields: " + Arrays.toString(projectedFields);
    }
    return TableConnectorUtils.generateRuntimeName(getClass(), getTableSchema().getFieldNames()) + explain;
  }
}

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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class BoundedTableFactory implements DynamicTableSourceFactory {
  private static final AtomicInteger DATA_SET_ID = new AtomicInteger(0);
  private static final Map<String, List<List<Row>>> DATA_SETS = Maps.newHashMap();

  private static final ConfigOption<String> DATA_ID =
      ConfigOptions.key("data-id").stringType().noDefaultValue();

  public static String registerDataSet(List<List<Row>> dataSet) {
    String dataSetId = String.valueOf(DATA_SET_ID.incrementAndGet());
    DATA_SETS.put(dataSetId, dataSet);
    return dataSetId;
  }

  public static void clearDataSets() {
    DATA_SETS.clear();
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    TableSchema tableSchema =
        TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

    Configuration configuration = Configuration.fromMap(context.getCatalogTable().getOptions());
    String dataId = configuration.getString(DATA_ID);
    Preconditions.checkArgument(
        DATA_SETS.containsKey(dataId), "data-id %s does not found in registered data set.", dataId);

    return new BoundedTableSource(DATA_SETS.get(dataId), tableSchema);
  }

  @Override
  public String factoryIdentifier() {
    return "BoundedSource";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return ImmutableSet.of();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return ImmutableSet.of(DATA_ID);
  }

  private static class BoundedTableSource implements ScanTableSource {

    private final List<List<Row>> elementsPerCheckpoint;
    private final TableSchema tableSchema;

    private BoundedTableSource(List<List<Row>> elementsPerCheckpoint, TableSchema tableSchema) {
      this.elementsPerCheckpoint = elementsPerCheckpoint;
      this.tableSchema = tableSchema;
    }

    private BoundedTableSource(BoundedTableSource toCopy) {
      this.elementsPerCheckpoint = toCopy.elementsPerCheckpoint;
      this.tableSchema = toCopy.tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
      Supplier<Stream<Row>> supplier = () -> elementsPerCheckpoint.stream().flatMap(List::stream);

      // Add the INSERT row kind by default.
      ChangelogMode.Builder builder = ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT);

      if (supplier.get().anyMatch(r -> r.getKind() == RowKind.DELETE)) {
        builder.addContainedKind(RowKind.DELETE);
      }

      if (supplier.get().anyMatch(r -> r.getKind() == RowKind.UPDATE_BEFORE)) {
        builder.addContainedKind(RowKind.UPDATE_BEFORE);
      }

      if (supplier.get().anyMatch(r -> r.getKind() == RowKind.UPDATE_AFTER)) {
        builder.addContainedKind(RowKind.UPDATE_AFTER);
      }

      return builder.build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
      return new DataStreamScanProvider() {
        @Override
        public DataStream<RowData> produceDataStream(
            ProviderContext providerContext, StreamExecutionEnvironment env) {
          boolean checkpointEnabled = env.getCheckpointConfig().isCheckpointingEnabled();
          SourceFunction<Row> source =
              new BoundedTestSource<>(elementsPerCheckpoint, checkpointEnabled);

          RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
          // Converter to convert the Row to RowData.
          DataFormatConverters.RowConverter rowConverter =
              new DataFormatConverters.RowConverter(tableSchema.getFieldDataTypes());

          return env.addSource(source, new RowTypeInfo(tableSchema.getFieldTypes()))
              .map(rowConverter::toInternal, FlinkCompatibilityUtil.toTypeInfo(rowType));
        }

        @Override
        public boolean isBounded() {
          return true;
        }
      };
    }

    @Override
    public DynamicTableSource copy() {
      return new BoundedTableSource(this);
    }

    @Override
    public String asSummaryString() {
      return "Bounded test table source";
    }
  }
}

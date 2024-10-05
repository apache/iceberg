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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkReadOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;

public class FlinkSource {
  private FlinkSource() {}

  /**
   * Initialize a {@link Builder} to read the data from iceberg table. Equivalent to {@link
   * TableScan}. See more options in {@link ScanContext}.
   *
   * <p>The Source can be read static data in bounded mode. It can also continuously check the
   * arrival of new data and read records incrementally.
   *
   * <ul>
   *   <li>Without startSnapshotId: Bounded
   *   <li>With startSnapshotId and with endSnapshotId: Bounded
   *   <li>With startSnapshotId (-1 means unbounded preceding) and Without endSnapshotId: Unbounded
   * </ul>
   *
   * <p>
   *
   * @return {@link Builder} to connect the iceberg table.
   */
  public static Builder forRowData() {
    return new Builder();
  }

  /** Source builder to build {@link DataStream}. */
  public static class Builder {
    private StreamExecutionEnvironment env;
    private Table table;
    private TableLoader tableLoader;
    private TableSchema projectedSchema;
    private ReadableConfig readableConfig = new Configuration();
    private final ScanContext.Builder contextBuilder = ScanContext.builder();
    private Boolean exposeLocality;

    private final Map<String, String> readOptions = Maps.newHashMap();

    public Builder tableLoader(TableLoader newLoader) {
      this.tableLoader = newLoader;
      return this;
    }

    public Builder table(Table newTable) {
      this.table = newTable;
      return this;
    }

    public Builder env(StreamExecutionEnvironment newEnv) {
      this.env = newEnv;
      return this;
    }

    public Builder filters(List<Expression> filters) {
      contextBuilder.filters(filters);
      return this;
    }

    public Builder project(TableSchema schema) {
      this.projectedSchema = schema;
      return this;
    }

    public Builder limit(Long newLimit) {
      if (newLimit != null) {
        readOptions.put(FlinkReadOptions.LIMIT, Long.toString(newLimit));
      }
      return this;
    }

    public Builder set(String property, String value) {
      readOptions.put(property, value);
      return this;
    }

    public Builder setAll(Map<String, String> properties) {
      readOptions.putAll(properties);
      return this;
    }

    /**
     * @deprecated Use {@link #setAll} instead.
     */
    @Deprecated
    public Builder properties(Map<String, String> properties) {
      readOptions.putAll(properties);
      return this;
    }

    public Builder caseSensitive(boolean caseSensitive) {
      readOptions.put(FlinkReadOptions.CASE_SENSITIVE, Boolean.toString(caseSensitive));
      return this;
    }

    public Builder snapshotId(Long snapshotId) {
      readOptions.put(FlinkReadOptions.SNAPSHOT_ID.key(), Long.toString(snapshotId));
      return this;
    }

    public Builder branch(String branch) {
      readOptions.put(FlinkReadOptions.BRANCH.key(), branch);
      return this;
    }

    public Builder tag(String tag) {
      readOptions.put(FlinkReadOptions.TAG.key(), tag);
      return this;
    }

    public Builder startSnapshotId(Long startSnapshotId) {
      readOptions.put(FlinkReadOptions.START_SNAPSHOT_ID.key(), Long.toString(startSnapshotId));
      return this;
    }

    public Builder endSnapshotId(Long endSnapshotId) {
      readOptions.put(FlinkReadOptions.END_SNAPSHOT_ID.key(), Long.toString(endSnapshotId));
      return this;
    }

    public Builder startTag(String startTag) {
      readOptions.put(FlinkReadOptions.START_TAG.key(), startTag);
      return this;
    }

    public Builder endTag(String endTag) {
      readOptions.put(FlinkReadOptions.END_TAG.key(), endTag);
      return this;
    }

    public Builder asOfTimestamp(Long asOfTimestamp) {
      readOptions.put(FlinkReadOptions.AS_OF_TIMESTAMP.key(), Long.toString(asOfTimestamp));
      return this;
    }

    public Builder splitSize(Long splitSize) {
      readOptions.put(FlinkReadOptions.SPLIT_SIZE, Long.toString(splitSize));
      return this;
    }

    public Builder splitLookback(Integer splitLookback) {
      readOptions.put(FlinkReadOptions.SPLIT_LOOKBACK, Integer.toString(splitLookback));
      return this;
    }

    public Builder splitOpenFileCost(Long splitOpenFileCost) {
      readOptions.put(FlinkReadOptions.SPLIT_FILE_OPEN_COST, Long.toString(splitOpenFileCost));
      return this;
    }

    public Builder streaming(boolean streaming) {
      readOptions.put(FlinkReadOptions.STREAMING, Boolean.toString(streaming));
      return this;
    }

    public Builder exposeLocality(boolean newExposeLocality) {
      this.exposeLocality = newExposeLocality;
      return this;
    }

    public Builder nameMapping(String nameMapping) {
      readOptions.put(TableProperties.DEFAULT_NAME_MAPPING, nameMapping);
      return this;
    }

    public Builder monitorInterval(Duration interval) {
      readOptions.put(FlinkReadOptions.MONITOR_INTERVAL, interval.toNanos() + " ns");
      return this;
    }

    public Builder maxPlanningSnapshotCount(int newMaxPlanningSnapshotCount) {
      readOptions.put(
          FlinkReadOptions.MAX_PLANNING_SNAPSHOT_COUNT,
          Integer.toString(newMaxPlanningSnapshotCount));
      return this;
    }

    public Builder flinkConf(ReadableConfig config) {
      this.readableConfig = config;
      return this;
    }

    public FlinkInputFormat buildFormat() {
      Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");

      Schema icebergSchema;
      FileIO io;
      EncryptionManager encryption;
      if (table == null) {
        // load required fields by table loader.
        tableLoader.open();
        try (TableLoader loader = tableLoader) {
          table = loader.loadTable();
          icebergSchema = table.schema();
          io = table.io();
          encryption = table.encryption();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      } else {
        icebergSchema = table.schema();
        io = table.io();
        encryption = table.encryption();
      }

      if (projectedSchema == null) {
        contextBuilder.project(icebergSchema);
      } else {
        contextBuilder.project(FlinkSchemaUtil.convert(icebergSchema, projectedSchema));
      }

      contextBuilder.exposeLocality(
          SourceUtil.isLocalityEnabled(table, readableConfig, exposeLocality));
      contextBuilder.planParallelism(
          readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_WORKER_POOL_SIZE));

      contextBuilder.resolveConfig(table, readOptions, readableConfig);

      ScanContext context = contextBuilder.build();
      context.validate();
      return new FlinkInputFormat(tableLoader, icebergSchema, io, encryption, context);
    }

    public DataStream<RowData> build() {
      Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");
      FlinkInputFormat format = buildFormat();

      ScanContext context = contextBuilder.build();
      TypeInformation<RowData> typeInfo =
          FlinkCompatibilityUtil.toTypeInfo(FlinkSchemaUtil.convert(context.project()));

      if (!context.isStreaming()) {
        int parallelism =
            SourceUtil.inferParallelism(
                readableConfig,
                context.limit(),
                () -> {
                  try {
                    return format.createInputSplits(0).length;
                  } catch (IOException e) {
                    throw new UncheckedIOException(
                        "Failed to create iceberg input splits for table: " + table, e);
                  }
                });
        if (env.getMaxParallelism() > 0) {
          parallelism = Math.min(parallelism, env.getMaxParallelism());
        }
        return env.createInput(format, typeInfo).setParallelism(parallelism);
      } else {
        StreamingMonitorFunction function = new StreamingMonitorFunction(tableLoader, context);

        String monitorFunctionName = String.format("Iceberg table (%s) monitor", table);
        String readerOperatorName = String.format("Iceberg table (%s) reader", table);

        return env.addSource(function, monitorFunctionName)
            .transform(readerOperatorName, typeInfo, StreamingReaderOperator.factory(format));
      }
    }
  }

  public static boolean isBounded(Map<String, String> properties) {
    return !PropertyUtil.propertyAsBoolean(properties, FlinkReadOptions.STREAMING, false);
  }
}

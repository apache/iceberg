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
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class FlinkSource {
  private FlinkSource() {
  }

  /**
   * Initialize a {@link Builder} to read the data from iceberg table. Equivalent to {@link TableScan}.
   * See more options in {@link ScanOptions}.
   * <p>
   * The Source can be read static data in bounded mode. It can also continuously check the arrival of new data and
   * read records incrementally.
   * The Bounded and Unbounded depends on the {@link Builder#options(ScanOptions)}:
   * <ul>
   *   <li>Without startSnapshotId: Bounded</li>
   *   <li>With startSnapshotId and with endSnapshotId: Bounded</li>
   *   <li>With startSnapshotId (-1 means unbounded preceding) and Without endSnapshotId: Unbounded</li>
   * </ul>
   * <p>
   *
   * @return {@link Builder} to connect the iceberg table.
   */
  public static Builder forRowData() {
    return new Builder();
  }

  /**
   * Source builder to build {@link DataStream}.
   */
  public static class Builder {
    private StreamExecutionEnvironment env;
    private Table table;
    private TableLoader tableLoader;
    private TableSchema projectedSchema;
    private ScanOptions options = ScanOptions.builder().build();
    private List<Expression> filterExpressions;
    private Configuration hadoopConf;

    private RowDataTypeInfo rowTypeInfo;

    public Builder tableLoader(TableLoader newLoader) {
      this.tableLoader = newLoader;
      return this;
    }

    public Builder table(Table newTable) {
      this.table = newTable;
      return this;
    }

    public Builder filters(List<Expression> newFilters) {
      this.filterExpressions = newFilters;
      return this;
    }

    public Builder project(TableSchema schema) {
      this.projectedSchema = schema;
      return this;
    }

    public Builder options(ScanOptions newOptions) {
      this.options = newOptions;
      return this;
    }

    public Builder hadoopConf(Configuration newConf) {
      this.hadoopConf = newConf;
      return this;
    }

    public Builder env(StreamExecutionEnvironment newEnv) {
      this.env = newEnv;
      return this;
    }

    public FlinkInputFormat buildFormat() {
      Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");

      hadoopConf = hadoopConf == null ? FlinkCatalogFactory.clusterHadoopConf() : hadoopConf;

      Schema icebergSchema;
      FileIO io;
      EncryptionManager encryption;
      if (table == null) {
        // load required fields by table loader.
        tableLoader.open(hadoopConf);
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

      rowTypeInfo = RowDataTypeInfo.of((RowType) (
          projectedSchema == null ?
              FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(icebergSchema)) :
              projectedSchema).toRowDataType().getLogicalType());

      Schema expectedSchema = icebergSchema;
      if (projectedSchema != null) {
        expectedSchema = FlinkSchemaUtil.convert(icebergSchema, projectedSchema);
      }

      return new FlinkInputFormat(tableLoader, icebergSchema, expectedSchema, io, encryption, filterExpressions,
          options, new SerializableConfiguration(hadoopConf));
    }

    public DataStream<RowData> build() {
      Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");
      FlinkInputFormat format = buildFormat();
      if (options.startSnapshotId() != null && options.endSnapshotId() == null) {
        throw new UnsupportedOperationException("The Unbounded mode is not supported yet");
      } else {
        return env.createInput(format, rowTypeInfo);
      }
    }
  }
}

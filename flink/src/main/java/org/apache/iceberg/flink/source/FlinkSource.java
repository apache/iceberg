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
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class FlinkSource {
  private FlinkSource() {
  }

  /**
   * Initialize a {@link Builder} to read the data from iceberg table in bounded mode. Reading a snapshot of the table.
   *
   * @return {@link Builder} to connect the iceberg table.
   */
  public static Builder forBounded() {
    return new BoundedBuilder();
  }

  /**
   * Source builder to build {@link DataStream}.
   */
  public abstract static class Builder {
    StreamExecutionEnvironment env;
    private Table table;
    private TableLoader tableLoader;
    private List<String> selectedFields;
    private TableSchema projectedSchema;
    private ScanOptions options = ScanOptions.builder().build();
    private List<Expression> filterExpressions;
    private org.apache.hadoop.conf.Configuration hadoopConf;

    private Schema expectedSchema;
    RowDataTypeInfo outputTypeInfo;

    // -------------------------- Required options -------------------------------

    public Builder tableLoader(TableLoader newLoader) {
      this.tableLoader = newLoader;
      return this;
    }

    // -------------------------- Optional options -------------------------------

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

    public Builder select(String... fields) {
      this.selectedFields = Lists.newArrayList(fields);
      return this;
    }

    public Builder select(List<String> fields) {
      this.selectedFields = fields;
      return this;
    }

    public Builder options(ScanOptions newOptions) {
      this.options = newOptions;
      return this;
    }

    public Builder hadoopConf(org.apache.hadoop.conf.Configuration newConf) {
      this.hadoopConf = newConf;
      return this;
    }

    public Builder env(StreamExecutionEnvironment newEnv) {
      this.env = newEnv;
      return this;
    }

    public FlinkInputFormat buildFormat() {
      Preconditions.checkNotNull(tableLoader, "TableLoader should not be null.");

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

      if (projectedSchema != null && selectedFields != null) {
        throw new IllegalArgumentException(
            "Cannot using both requestedSchema and projectedFields to project.");
      }

      TableSchema projectedTableSchema = projectedSchema;
      TableSchema tableSchema = FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(icebergSchema));
      if (selectedFields != null) {
        TableSchema.Builder builder = TableSchema.builder();
        for (String field : selectedFields) {
          TableColumn column = tableSchema.getTableColumn(field).orElseThrow(
              () -> new IllegalArgumentException(String.format("The field(%s) can not be found in the table schema: %s",
                  field, tableSchema)));
          builder.field(column.getName(), column.getType());
        }
        projectedTableSchema = builder.build();
      }

      outputTypeInfo = RowDataTypeInfo.of((RowType) (projectedTableSchema == null ? tableSchema : projectedTableSchema)
              .toRowDataType().getLogicalType());

      expectedSchema = icebergSchema;
      if (projectedTableSchema != null) {
        expectedSchema = FlinkSchemaUtil.convert(icebergSchema, projectedTableSchema);
      }

      return new FlinkInputFormat(tableLoader, expectedSchema, io, encryption, filterExpressions, options,
          new SerializableConfiguration(hadoopConf));
    }

    public abstract DataStream<RowData> build();
  }

  private static final class BoundedBuilder extends Builder {
    @Override
    public DataStream<RowData> build() {
      Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");
      FlinkInputFormat format = buildFormat();
      return env.createInput(format, outputTypeInfo);
    }
  }
}

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
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Flink {@link InputFormat} for Iceberg.
 */
public class FlinkInputFormat extends RichInputFormat<RowData, FlinkInputSplit> {

  private static final long serialVersionUID = 1L;

  private final TableLoader tableLoader;
  private final Schema projectedSchema;
  private final ScanOptions options;
  private final List<Expression> filterExpressions;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final SerializableConfiguration serializableConf;

  private transient RowDataIterator iterator;

  private FlinkInputFormat(
      TableLoader tableLoader, Schema projectedSchema, FileIO io, EncryptionManager encryption,
      List<Expression> filterExpressions, ScanOptions options, SerializableConfiguration serializableConf) {
    this.tableLoader = tableLoader;
    this.projectedSchema = projectedSchema;
    this.options = options;
    this.filterExpressions = filterExpressions;
    this.io = io;
    this.encryption = encryption;
    this.serializableConf = serializableConf;
  }

  @VisibleForTesting
  Schema projectedSchema() {
    return projectedSchema;
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
    // Legacy method, not be used.
    return null;
  }

  @Override
  public FlinkInputSplit[] createInputSplits(int minNumSplits) throws IOException {
    // Called in Job manager, so it is OK to load table from catalog.
    tableLoader.open(serializableConf.get());
    try (TableLoader loader = tableLoader) {
      Table table = loader.loadTable();
      FlinkSplitGenerator generator = new FlinkSplitGenerator(table, projectedSchema, options, filterExpressions);
      return generator.createInputSplits();
    }
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(FlinkInputSplit[] inputSplits) {
    return new DefaultInputSplitAssigner(inputSplits);
  }

  @Override
  public void configure(Configuration parameters) {
  }

  @Override
  public void open(FlinkInputSplit split) {
    this.iterator = new RowDataIterator(split.getTask(), io, encryption, projectedSchema,
                                        options.getNameMapping(), options.isCaseSensitive());
  }

  @Override
  public boolean reachedEnd() {
    return !iterator.hasNext();
  }

  @Override
  public RowData nextRecord(RowData reuse) {
    return iterator.next();
  }

  @Override
  public void close() throws IOException {
    if (iterator != null) {
      iterator.close();
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private Table table;
    private TableLoader tableLoader;
    private List<String> selectedFields;
    private TableSchema projectedSchema;
    private ScanOptions options = ScanOptions.builder().build();
    private List<Expression> filterExpressions;
    private org.apache.hadoop.conf.Configuration hadoopConf;

    private Builder() {
    }

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

    public FlinkInputFormat build() {
      Preconditions.checkNotNull(tableLoader, "TableLoader should not be null.");

      hadoopConf = hadoopConf == null ? FlinkCatalogFactory.clusterHadoopConf() : hadoopConf;

      Schema icebergSchema;
      FileIO io;
      EncryptionManager encryption;
      if (table == null) {
        // load required fields by table loader.
        tableLoader.open(hadoopConf);
        try (TableLoader loader = tableLoader) {
          Table loadedTable = loader.loadTable();
          icebergSchema = loadedTable.schema();
          io = loadedTable.io();
          encryption = loadedTable.encryption();
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

      TableSchema flinkProjectedSchema = projectedSchema;

      if (selectedFields != null) {
        TableSchema tableSchema = FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(icebergSchema));
        TableSchema.Builder builder = TableSchema.builder();
        for (String field : selectedFields) {
          TableColumn column = tableSchema.getTableColumn(field).orElseThrow(
              () -> new IllegalArgumentException(String.format("The field(%s) can not be found in the table schema: %s",
                                                               field, tableSchema)));
          builder.field(column.getName(), column.getType());
        }
        flinkProjectedSchema = builder.build();
      }

      Schema icebergProjectedSchema = icebergSchema;
      if (flinkProjectedSchema != null) {
        icebergProjectedSchema = FlinkSchemaUtil.convert(icebergSchema, flinkProjectedSchema);
      }

      return new FlinkInputFormat(tableLoader, icebergProjectedSchema, io, encryption, filterExpressions, options,
                                  new SerializableConfiguration(hadoopConf));
    }
  }
}

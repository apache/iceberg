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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataTableSource {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataTableSource.class);

  private MetadataTableSource() {
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private StreamExecutionEnvironment env;
    private String tableName;
    private TableOperations tableOps;
    private MetadataTableType metadataTableType;
    private int maxParallelism = Integer.MAX_VALUE;

    public Builder env(StreamExecutionEnvironment environment) {
      this.env = environment;
      return this;
    }

    public Builder tableName(String name) {
      this.tableName = name;
      return this;
    }

    public Builder tableOperations(TableOperations ops) {
      this.tableOps = ops;
      return this;
    }

    public Builder metadataTableType(MetadataTableType type) {
      this.metadataTableType = type;
      return this;
    }

    public Builder maxParallelism(int parallelism) {
      Preconditions.checkArgument(parallelism > 0, "Invalid max parallelism %d", parallelism);
      this.maxParallelism = parallelism;
      return this;
    }

    public DataStream<RowData> build() {
      Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");
      Preconditions.checkNotNull(tableName, "TableName should not be null");

      Table metadataTable = MetadataTableUtils.createMetadataTableInstance(tableOps, tableName,
          metadataTableType.name(), metadataTableType);

      Schema schema = metadataTable.schema();
      FileIO io = metadataTable.io();
      EncryptionManager encryptionManager = metadataTable.encryption();
      String nameMapping = null;
      boolean caseSensitive = false;
      MetadataTableMap map = new MetadataTableMap(tableName, schema, io, encryptionManager, nameMapping, caseSensitive);

      List<CombinedScanTask> combinedScanTasks = Lists.newArrayList(metadataTable.newScan().planTasks().iterator());
      int parallelism = Math.min(combinedScanTasks.size(), maxParallelism);
      return env.fromCollection(combinedScanTasks).setParallelism(parallelism)
          .flatMap(map).setParallelism(parallelism);
    }
  }

  private static class MetadataTableMap extends RichFlatMapFunction<CombinedScanTask, RowData> {

    private final String name;
    private final FileIO io;
    private final EncryptionManager encryptionManager;
    private final RowDataFileScanTaskReader rowDataReader;

    private MetadataTableMap(
        String name,
        Schema schema,
        FileIO io,
        EncryptionManager encryptionManager,
        String nameMapping,
        boolean caseSensitive) {
      this.name = name;
      this.io = io;
      this.encryptionManager = encryptionManager;
      this.rowDataReader = new RowDataFileScanTaskReader(schema, schema, nameMapping, caseSensitive);
    }

    @Override
    public void flatMap(CombinedScanTask task, Collector<RowData> out) throws Exception {

      try (DataIterator<RowData> iterator =
               new DataIterator<>(rowDataReader, task, io, encryptionManager)) {
        while (iterator.hasNext()) {
          RowData rowData = iterator.next();
          out.collect(rowData);
        }
      } catch (Exception e) {
        LOG.error("Failed to read metadata table: " + name);
        throw e;
      }
    }
  }
}

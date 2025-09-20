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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.Map;
import java.util.function.Predicate;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.FileSystemWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recursively lists the files in the directory. Hidden files, and files younger than the `minAgeMs`
 * are omitted in the result.
 */
@Internal
public class ListFileSystemFilesForDir
    extends ProcessFunction<ListFileSystemFilesForDir.OrphanFilesDirTask, String> {
  private static final Logger LOG = LoggerFactory.getLogger(ListFileSystemFilesForDir.class);

  private final String taskName;
  private final int taskIndex;

  private Map<Integer, PartitionSpec> specs;
  private transient Counter errorCounter;
  private final TableLoader tableLoader;
  private transient Configuration configuration;

  public ListFileSystemFilesForDir(String taskName, int taskIndex, TableLoader tableLoader) {
    Preconditions.checkNotNull(taskName, "Task name should no be null");
    Preconditions.checkNotNull(tableLoader, "TableLoad should no be null");

    this.tableLoader = tableLoader;
    this.taskName = taskName;
    this.taskIndex = taskIndex;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    tableLoader.open();
    Table table = tableLoader.loadTable();
    this.specs = table.specs();
    this.errorCounter =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), table.name(), taskName, taskIndex)
            .counter(TableMaintenanceMetrics.ERROR_COUNTER);
    this.configuration = new Configuration();
    table.properties().forEach(configuration::set);
  }

  @Override
  public void processElement(
      OrphanFilesDirTask orphanFilesDirTask, Context ctx, Collector<String> out) throws Exception {

    try {
      Predicate<FileStatus> predicate =
          file -> file.getModificationTime() < orphanFilesDirTask.olderThanTimestamp();
      FileSystemWalker.listDirRecursivelyWithHadoop(
          orphanFilesDirTask.subDir(),
          specs,
          predicate,
          configuration,
          Integer.MAX_VALUE,
          Integer.MAX_VALUE,
          dir -> {},
          out::collect);
    } catch (Exception e) {
      LOG.warn(
          "Exception listing files for {} at {}", orphanFilesDirTask.subDir(), ctx.timestamp(), e);
      ctx.output(DeleteOrphanFiles.ERROR_STREAM, e);
      errorCounter.inc();
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
  }

  public static class OrphanFilesDirTask {

    private String dir;
    private long olderThanTimestamp;

    public OrphanFilesDirTask() {}

    public OrphanFilesDirTask(String dir, long olderThanTimestamp) {
      this.dir = dir;
      this.olderThanTimestamp = olderThanTimestamp;
    }

    public String subDir() {
      return dir;
    }

    public long olderThanTimestamp() {
      return olderThanTimestamp;
    }
  }
}

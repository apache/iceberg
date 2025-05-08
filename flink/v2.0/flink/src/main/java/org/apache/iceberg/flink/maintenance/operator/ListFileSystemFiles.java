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

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.PathFilter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.PartitionAwareHiddenPathFilter;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.FileSystemWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recursively lists the files in the `location` directory. Hidden files, and files younger than the
 * `minAgeMs` are omitted in the result.
 */
@Internal
public class ListFileSystemFiles extends ProcessFunction<Trigger, String> {
  private static final Logger LOG = LoggerFactory.getLogger(ListFileSystemFiles.class);

  private final String taskName;
  private final int taskIndex;

  private SupportsPrefixOperations io;
  private Map<Integer, PartitionSpec> specs;
  private String location;
  private final long minAgeMs;
  private transient Counter errorCounter;
  private final TableLoader tableLoader;

  public ListFileSystemFiles(
      String taskName, int taskIndex, TableLoader tableLoader, String location, long minAgeMs) {
    Preconditions.checkNotNull(taskName, "Task name should no be null");
    Preconditions.checkNotNull(tableLoader, "TableLoad should no be null");

    this.tableLoader = tableLoader;
    this.taskName = taskName;
    this.taskIndex = taskIndex;
    this.minAgeMs = minAgeMs;
    this.location = location;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    tableLoader.open();
    Table table = tableLoader.loadTable();
    Preconditions.checkArgument(
        table.io() instanceof SupportsPrefixOperations,
        "Cannot use prefix listing with FileIO {} which does not support prefix operations.",
        table.io());

    this.io = (SupportsPrefixOperations) table.io();
    this.location = location != null ? location : table.location();
    this.specs = table.specs();
    this.errorCounter =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), table.name(), taskName, taskIndex)
            .counter(TableMaintenanceMetrics.ERROR_COUNTER);
  }

  @Override
  public void processElement(Trigger trigger, Context ctx, Collector<String> out) throws Exception {
    long olderThanTimestamp = trigger.timestamp() - minAgeMs;
    try {
      PathFilter filter = PartitionAwareHiddenPathFilter.forSpecs(specs);
      Predicate<FileInfo> predicate = fileInfo -> fileInfo.createdAtMillis() < olderThanTimestamp;
      List<String> matchingFiles =
          FileSystemWalker.listDirRecursively(io, location, predicate, filter);
      matchingFiles.forEach(out::collect);
    } catch (Exception e) {
      LOG.error("Exception listing files for {} at {}", location, ctx.timestamp(), e);
      ctx.output(DeleteOrphanFiles.ERROR_STREAM, e);
      errorCounter.inc();
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
  }
}

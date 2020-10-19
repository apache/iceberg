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

package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReadFileFilter;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK;
import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE_DEFAULT;

public class SparkFileBatchScan extends BaseBatchScan implements SupportsReadFileFilter {
  private List<FileScanTask> files;

  // lazy cache of tasks
  private List<CombinedScanTask> tasks = null;

  public SparkFileBatchScan(Table table, List<FileScanTask> files, Broadcast<FileIO> io,
                            Broadcast<EncryptionManager> encryptionManager, boolean caseSensitive,
                            CaseInsensitiveStringMap options) {
    super(table, table.schema(), io, encryptionManager, caseSensitive, options);
    this.files = files;
  }

  @Override
  public void filterFiles(Set<String> locations) {
    // invalidate cached tasks to trigger split planning again
    tasks = null;
    files = files.stream()
        .filter(file -> locations.contains(file.file().path().toString()))
        .collect(Collectors.toList());
  }

  @Override
  public Statistics estimateStatistics() {
    // its a fresh table, no data
    if (table.currentSnapshot() == null) {
      return new Stats(0L, 0L);
    }

    long sizeInBytes = 0L;
    long numRows = 0L;

    for (CombinedScanTask task : tasks()) {
      for (FileScanTask file : task.files()) {
        sizeInBytes += file.length();
        numRows += file.file().recordCount();
      }
    }

    return new Stats(sizeInBytes, numRows);
  }


  @Override
  protected List<CombinedScanTask> tasks() {
    if (tasks == null) {
      Map<String, String> props = table.properties();

      long actualSplitSize;
      if (splitSize != null) {
        actualSplitSize = splitSize;
      } else {
        actualSplitSize = PropertyUtil.propertyAsLong(props, SPLIT_SIZE, SPLIT_SIZE_DEFAULT);
      }

      int actualSplitLookback;
      if (splitLookback != null) {
        actualSplitLookback = splitLookback;
      } else {
        actualSplitLookback = PropertyUtil.propertyAsInt(props, SPLIT_LOOKBACK, SPLIT_LOOKBACK_DEFAULT);
      }

      long actualOpenFileCost;
      if (splitOpenFileCost != null) {
        actualOpenFileCost = splitOpenFileCost;
      } else {
        actualOpenFileCost = PropertyUtil.propertyAsLong(props, SPLIT_OPEN_FILE_COST, SPLIT_OPEN_FILE_COST_DEFAULT);
      }

      CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(
          CloseableIterable.withNoopClose(files),
          actualSplitSize);
      CloseableIterable<CombinedScanTask> scanTasks = TableScanUtil.planTasks(
          splitFiles, actualSplitSize,
          actualSplitLookback, actualOpenFileCost);
      tasks = Lists.newArrayList(scanTasks);
    }

    return tasks;
  }

  @Override
  public String description() {
    return String.format("File scan on %s [num_files=%d]", table, files.size());
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergFileScan(table=%s, type=%s, num_files=%d, caseSensitive=%s)",
        table, expectedSchema.asStruct(), files.size(), caseSensitive);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkFileBatchScan that = (SparkFileBatchScan) o;
    return table.toString().equals(that.table.toString()) &&
        readSchema().equals(that.readSchema()) &&
        files.equals(that.files);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table.toString(), readSchema(), files);
  }
}

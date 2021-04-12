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
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.DataCompactionJobCoordinator;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK;
import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE_DEFAULT;

public class SparkCompactionScan extends SparkBatchScan {
  private final String jobID;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;

  private List<CombinedScanTask> tasks = null; // lazy cache of tasks

  public SparkCompactionScan(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryption,
                             boolean caseSensitive, CaseInsensitiveStringMap options) {
    super(table, io, encryption, caseSensitive, table.schema(), ImmutableList.of(), options);

    this.jobID = options.get(SparkReadOptions.COMPACTION_JOB_ID);

    Map<String, String> props = table.properties();

    long tableSplitSize = PropertyUtil.propertyAsLong(props, SPLIT_SIZE, SPLIT_SIZE_DEFAULT);
    this.splitSize = Spark3Util.propertyAsLong(options, SparkReadOptions.SPLIT_SIZE, tableSplitSize);

    int tableSplitLookback = PropertyUtil.propertyAsInt(props, SPLIT_LOOKBACK, SPLIT_LOOKBACK_DEFAULT);
    this.splitLookback = Spark3Util.propertyAsInt(options, SparkReadOptions.LOOKBACK, tableSplitLookback);

    long tableOpenFileCost = PropertyUtil.propertyAsLong(props, SPLIT_OPEN_FILE_COST, SPLIT_OPEN_FILE_COST_DEFAULT);
    this.splitOpenFileCost = Spark3Util.propertyAsLong(options, SparkReadOptions.FILE_OPEN_COST, tableOpenFileCost);
  }

  @Override
  protected List<CombinedScanTask> tasks() {
    if (tasks == null) {
      List<FileScanTask> files = DataCompactionJobCoordinator.getTasks(table(), jobID);
      Preconditions.checkArgument(files != null, "Cannot find files to compact for job %s", jobID);

      CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(
          CloseableIterable.withNoopClose(files),
          splitSize);
      CloseableIterable<CombinedScanTask> scanTasks = TableScanUtil.planTasks(
          splitFiles, splitSize,
          splitLookback, splitOpenFileCost);
      this.tasks = Lists.newArrayList(scanTasks);
    }

    return tasks;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    SparkCompactionScan that = (SparkCompactionScan) other;
    return table().name().equals(that.table().name()) &&
        Objects.equals(jobID, that.jobID) &&
        Objects.equals(splitSize, that.splitSize) &&
        Objects.equals(splitLookback, that.splitLookback) &&
        Objects.equals(splitOpenFileCost, that.splitOpenFileCost);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table().name(), jobID, splitSize, splitSize, splitOpenFileCost);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergCompactionScan(table=%s, type=%s, jobID=%s, caseSensitive=%s)",
        table(), expectedSchema().asStruct(), jobID, caseSensitive());
  }
}

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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK;
import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE_DEFAULT;

class SparkMergeScan extends SparkBatchScan {

  private final Table table;
  private final boolean ignoreResiduals;
  private final Schema expectedSchema;
  private final Long snapshotId;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;

  // lazy variables
  private List<FileScanTask> files = null; // lazy cache of files
  private List<CombinedScanTask> tasks = null; // lazy cache of tasks

  SparkMergeScan(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryption,
                 boolean caseSensitive, boolean ignoreResiduals, Schema expectedSchema,
                 List<Expression> filters, CaseInsensitiveStringMap options) {

    super(table, io, encryption, caseSensitive, expectedSchema, filters, options);

    this.table = table;
    this.ignoreResiduals = ignoreResiduals;
    this.expectedSchema = expectedSchema;
    this.snapshotId = Spark3Util.propertyAsLong(options, "snapshot-id", null);

    Map<String, String> props = table.properties();

    long tableSplitSize = PropertyUtil.propertyAsLong(props, SPLIT_SIZE, SPLIT_SIZE_DEFAULT);
    this.splitSize = Spark3Util.propertyAsLong(options, "split-size", tableSplitSize);

    int tableSplitLookback = PropertyUtil.propertyAsInt(props, SPLIT_LOOKBACK, SPLIT_LOOKBACK_DEFAULT);
    this.splitLookback = Spark3Util.propertyAsInt(options, "lookback", tableSplitLookback);

    long tableOpenFileCost = PropertyUtil.propertyAsLong(props, SPLIT_OPEN_FILE_COST, SPLIT_OPEN_FILE_COST_DEFAULT);
    this.splitOpenFileCost = Spark3Util.propertyAsLong(options, "file-open-cost", tableOpenFileCost);

    if (snapshotId == null) {
      // init files with an empty list if the snapshot id is not set to avoid picking any concurrent changes
      files = Collections.emptyList();
    }
  }

  Long snapshotId() {
    return snapshotId;
  }

  @Override
  public Statistics estimateStatistics() {
    if (snapshotId == null) {
      return new Stats(0L, 0L);
    }
    return super.estimateStatistics();
  }

  // should be accessible to the write
  List<FileScanTask> files() {
    if (files == null) {
      TableScan scan = table
          .newScan()
          .caseSensitive(caseSensitive())
          .useSnapshot(snapshotId)
          .project(expectedSchema);

      for (Expression filter : filterExpressions()) {
        scan = scan.filter(filter);
      }

      if (ignoreResiduals) {
        scan.ignoreResiduals();
      }

      try (CloseableIterable<FileScanTask> filesIterable = scan.planFiles()) {
        this.files = Lists.newArrayList(filesIterable);
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to close table scan: %s", scan);
      }
    }

    return files;
  }

  @Override
  protected List<CombinedScanTask> tasks() {
    if (tasks == null) {
      CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(
          CloseableIterable.withNoopClose(files()),
          splitSize);
      CloseableIterable<CombinedScanTask> scanTasks = TableScanUtil.planTasks(
          splitFiles, splitSize,
          splitLookback, splitOpenFileCost);
      tasks = Lists.newArrayList(scanTasks);
    }

    return tasks;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    // TODO: add filtered files
    SparkMergeScan that = (SparkMergeScan) o;
    return table().name().equals(that.table().name()) &&
        readSchema().equals(that.readSchema()) && // compare Spark schemas to ignore field ids
        filterExpressions().toString().equals(that.filterExpressions().toString()) &&
        ignoreResiduals == that.ignoreResiduals &&
        Objects.equals(snapshotId, that.snapshotId);
  }

  @Override
  public int hashCode() {
    // TODO: add filtered files
    return Objects.hash(table().name(), readSchema(), filterExpressions().toString(), ignoreResiduals, snapshotId);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergMergeScan(table=%s, type=%s, filters=%s, caseSensitive=%s)",
        table(), expectedSchema().asStruct(), filterExpressions(), caseSensitive());
  }
}

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
import java.util.Objects;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.SparkSession;

class SparkStagedScan extends SparkScan {

  private final String taskSetId;
  private final long splitSize;
  private final int splitLookback;
  private final long openFileCost;

  private List<ScanTaskGroup<ScanTask>> taskGroups = null; // lazy cache of tasks

  SparkStagedScan(SparkSession spark, Table table, SparkReadConf readConf) {
    this(spark, table, table.schema(), readConf);
  }

  SparkStagedScan(SparkSession spark, Table table, Schema expectedSchema, SparkReadConf readConf) {
    super(spark, table, readConf, expectedSchema, ImmutableList.of());
    this.taskSetId = readConf.scanTaskSetId();
    this.splitSize = readConf.splitSize();
    this.splitLookback = readConf.splitLookback();
    this.openFileCost = readConf.splitOpenFileCost();
  }

  @Override
  protected List<ScanTaskGroup<ScanTask>> taskGroups() {
    if (taskGroups == null) {
      ScanTaskSetManager taskSetManager = ScanTaskSetManager.get();
      List<ScanTask> tasks = taskSetManager.fetchTasks(table(), taskSetId);
      ValidationException.check(
          tasks != null,
          "Task set manager has no tasks for table %s with task set ID %s",
          table(),
          taskSetId);

      this.taskGroups = TableScanUtil.planTaskGroups(tasks, splitSize, splitLookback, openFileCost);
    }
    return taskGroups;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    SparkStagedScan that = (SparkStagedScan) other;
    return table().name().equals(that.table().name())
        && Objects.equals(taskSetId, that.taskSetId)
        && readSchema().equals(that.readSchema())
        && Objects.equals(splitSize, that.splitSize)
        && Objects.equals(splitLookback, that.splitLookback)
        && Objects.equals(openFileCost, that.openFileCost);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(), taskSetId, readSchema(), splitSize, splitSize, openFileCost);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergStagedScan(table=%s, type=%s, taskSetID=%s, caseSensitive=%s)",
        table(), expectedSchema().asStruct(), taskSetId, caseSensitive());
  }
}

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
package org.apache.iceberg.flink.maintenance.stream;

import java.time.Duration;
import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.TaskResult;
import org.apache.iceberg.flink.maintenance.operator.Trigger;
import org.apache.iceberg.flink.maintenance.operator.TriggerEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public abstract class MaintenanceTaskBuilder<T extends MaintenanceTaskBuilder> {
  private int index;
  private String name;
  private TableLoader tableLoader;
  private String uidSuffix = null;
  private String slotSharingGroup = null;
  private Integer parallelism = null;
  private TriggerEvaluator.Builder triggerEvaluator = new TriggerEvaluator.Builder();

  abstract DataStream<TaskResult> append(DataStream<Trigger> sourceStream);

  /**
   * After a given number of Iceberg table commits since the last run, starts the downstream job.
   *
   * @param commitCount after the downstream job should be started
   * @return for chained calls
   */
  public T scheduleOnCommitCount(int commitCount) {
    triggerEvaluator.commitCount(commitCount);
    return (T) this;
  }

  /**
   * After a given number of new data files since the last run, starts the downstream job.
   *
   * @param dataFileCount after the downstream job should be started
   * @return for chained calls
   */
  public T scheduleOnDataFileCount(int dataFileCount) {
    triggerEvaluator.dataFileCount(dataFileCount);
    return (T) this;
  }

  /**
   * After a given aggregated data file size since the last run, starts the downstream job.
   *
   * @param dataFileSizeInBytes after the downstream job should be started
   * @return for chained calls
   */
  public T scheduleOnDataFileSize(long dataFileSizeInBytes) {
    triggerEvaluator.dataFileSizeInBytes(dataFileSizeInBytes);
    return (T) this;
  }

  /**
   * After a given number of new positional delete files since the last run, starts the downstream
   * job.
   *
   * @param posDeleteFileCount after the downstream job should be started
   * @return for chained calls
   */
  public T scheduleOnPosDeleteFileCount(int posDeleteFileCount) {
    triggerEvaluator.posDeleteFileCount(posDeleteFileCount);
    return (T) this;
  }

  /**
   * After a given number of new positional delete records since the last run, starts the downstream
   * job.
   *
   * @param posDeleteRecordCount after the downstream job should be started
   * @return for chained calls
   */
  public T scheduleOnPosDeleteRecordCount(long posDeleteRecordCount) {
    triggerEvaluator.posDeleteRecordCount(posDeleteRecordCount);
    return (T) this;
  }

  /**
   * After a given number of new equality delete files since the last run, starts the downstream
   * job.
   *
   * @param eqDeleteFileCount after the downstream job should be started
   * @return for chained calls
   */
  public T scheduleOnEqDeleteFileCount(int eqDeleteFileCount) {
    triggerEvaluator.eqDeleteFileCount(eqDeleteFileCount);
    return (T) this;
  }

  /**
   * After a given number of new equality delete records since the last run, starts the downstream
   * job.
   *
   * @param eqDeleteRecordCount after the downstream job should be started
   * @return for chained calls
   */
  public T scheduleOnEqDeleteRecordCount(long eqDeleteRecordCount) {
    triggerEvaluator.eqDeleteRecordCount(eqDeleteRecordCount);
    return (T) this;
  }

  /**
   * After a given time since the last run, starts the downstream job.
   *
   * @param interval after the downstream job should be started
   * @return for chained calls
   */
  public T scheduleOnInterval(Duration interval) {
    triggerEvaluator.timeout(interval);
    return (T) this;
  }

  /**
   * The suffix used for the generated {@link org.apache.flink.api.dag.Transformation}'s uid.
   *
   * @param newUidSuffix for the transformations
   * @return for chained calls
   */
  public T uidSuffix(String newUidSuffix) {
    this.uidSuffix = newUidSuffix;
    return (T) this;
  }

  /**
   * The {@link SingleOutputStreamOperator#slotSharingGroup(String)} for all the operators of the
   * generated stream. Could be used to separate the resources used by this task.
   *
   * @param newSlotSharingGroup to be used for the operators
   * @return for chained calls
   */
  public T slotSharingGroup(String newSlotSharingGroup) {
    this.slotSharingGroup = newSlotSharingGroup;
    return (T) this;
  }

  /**
   * Sets the parallelism for the stream.
   *
   * @param newParallelism the required parallelism
   * @return for chained calls
   */
  public T parallelism(int newParallelism) {
    this.parallelism = newParallelism;
    return (T) this;
  }

  @Internal
  int id() {
    return index;
  }

  @Internal
  String name() {
    return name;
  }

  @Internal
  TableLoader tableLoader() {
    return tableLoader;
  }

  @Internal
  String uidSuffix() {
    return uidSuffix;
  }

  @Internal
  String slotSharingGroup() {
    return slotSharingGroup;
  }

  @Internal
  Integer parallelism() {
    return parallelism;
  }

  @Internal
  TriggerEvaluator evaluator() {
    return triggerEvaluator.build();
  }

  @Internal
  DataStream<TaskResult> append(
      DataStream<Trigger> sourceStream,
      int maintenanceTaskIndex,
      String maintainanceTaskName,
      TableLoader newTableLoader,
      String mainUidSuffix,
      String mainSlotSharingGroup,
      int mainParallelism) {
    Preconditions.checkArgument(
        parallelism == null || parallelism == -1 || parallelism > 0,
        "Parallelism should be left to default (-1/null) or greater than 0");
    Preconditions.checkNotNull(maintainanceTaskName, "Name should not be null");
    Preconditions.checkNotNull(newTableLoader, "TableLoader should not be null");

    this.index = maintenanceTaskIndex;
    this.name = maintainanceTaskName;
    this.tableLoader = newTableLoader;

    if (uidSuffix == null) {
      uidSuffix = name + "_" + index + "_" + mainUidSuffix;
    }

    if (parallelism == null) {
      parallelism = mainParallelism;
    }

    if (slotSharingGroup == null) {
      slotSharingGroup = mainSlotSharingGroup;
    }

    tableLoader.open();

    return append(sourceStream);
  }
}

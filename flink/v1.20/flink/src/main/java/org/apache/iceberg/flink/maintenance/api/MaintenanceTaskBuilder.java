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
package org.apache.iceberg.flink.maintenance.api;

import java.time.Duration;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.TriggerEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

@PublicEvolving
public abstract class MaintenanceTaskBuilder<T extends MaintenanceTaskBuilder<?>> {
  private int index;
  private String name;
  private TableLoader tableLoader;
  private String uidSuffix = null;
  private String slotSharingGroup = null;
  private Integer parallelism = null;
  private final TriggerEvaluator.Builder triggerEvaluator = new TriggerEvaluator.Builder();

  abstract DataStream<TaskResult> append(DataStream<Trigger> sourceStream);

  /**
   * After a given number of Iceberg table commits since the last run, starts the downstream job.
   *
   * @param commitCount after the downstream job should be started
   */
  public T scheduleOnCommitCount(int commitCount) {
    triggerEvaluator.commitCount(commitCount);
    return (T) this;
  }

  /**
   * After a given number of new data files since the last run, starts the downstream job.
   *
   * @param dataFileCount after the downstream job should be started
   */
  public T scheduleOnDataFileCount(int dataFileCount) {
    triggerEvaluator.dataFileCount(dataFileCount);
    return (T) this;
  }

  /**
   * After a given aggregated data file size since the last run, starts the downstream job.
   *
   * @param dataFileSizeInBytes after the downstream job should be started
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
   */
  public T scheduleOnEqDeleteRecordCount(long eqDeleteRecordCount) {
    triggerEvaluator.eqDeleteRecordCount(eqDeleteRecordCount);
    return (T) this;
  }

  /**
   * After a given time since the last run, starts the downstream job.
   *
   * @param interval after the downstream job should be started
   */
  public T scheduleOnInterval(Duration interval) {
    triggerEvaluator.timeout(interval);
    return (T) this;
  }

  /**
   * The suffix used for the generated {@link org.apache.flink.api.dag.Transformation}'s uid.
   *
   * @param newUidSuffix for the transformations
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
   */
  public T slotSharingGroup(String newSlotSharingGroup) {
    this.slotSharingGroup = newSlotSharingGroup;
    return (T) this;
  }

  /**
   * Sets the parallelism for the stream.
   *
   * @param newParallelism the required parallelism
   */
  public T parallelism(int newParallelism) {
    OperatorValidationUtils.validateParallelism(newParallelism);
    this.parallelism = newParallelism;
    return (T) this;
  }

  protected int index() {
    return index;
  }

  protected String name() {
    return name;
  }

  protected TableLoader tableLoader() {
    return tableLoader;
  }

  protected String uidSuffix() {
    return uidSuffix;
  }

  protected String slotSharingGroup() {
    return slotSharingGroup;
  }

  protected Integer parallelism() {
    return parallelism;
  }

  protected String operatorName(String operatorNameBase) {
    return operatorNameBase + "[" + index() + "]";
  }

  @Internal
  TriggerEvaluator evaluator() {
    return triggerEvaluator.build();
  }

  @Internal
  DataStream<TaskResult> append(
      DataStream<Trigger> sourceStream,
      int defaultTaskIndex,
      String defaultTaskName,
      TableLoader newTableLoader,
      String mainUidSuffix,
      String mainSlotSharingGroup,
      int mainParallelism) {
    Preconditions.checkNotNull(defaultTaskName, "Name should not be null");
    Preconditions.checkNotNull(newTableLoader, "TableLoader should not be null");

    this.index = defaultTaskIndex;
    this.name = defaultTaskName;
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

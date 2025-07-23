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
package org.apache.iceberg.flink.actions;

import java.util.List;
import java.util.UUID;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.MaintenanceTaskBuilder;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.flink.maintenance.api.TaskResult;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class BaseTableMaintenanceAction<T> {

  private final StreamExecutionEnvironment env;
  private final TableLoader tableLoader;
  private final long triggerTimestamp;
  private static final String DEFAULT_UID_SUFFIX = UUID.randomUUID().toString();
  private static final int DEFAULT_TASK_INDEX = 0;
  private final MaintenanceTaskBuilder<?> builder;

  public BaseTableMaintenanceAction(
      StreamExecutionEnvironment env,
      TableLoader tableLoader,
      MaintenanceTaskBuilder<?> builder,
      long triggerTimestamp) {
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");
    Preconditions.checkNotNull(builder, "Builder should not be null");

    this.env = env;
    this.tableLoader = tableLoader;
    this.triggerTimestamp = triggerTimestamp;
    this.builder = builder;
  }

  /**
   * Executes the maintenance task and returns the first task result.
   *
   * @return {@link TaskResult} from the execution, or null if no results were produced
   */
  public TaskResult execute() {
    String tableName = tableLoader.loadTable().name();
    DataStream<TaskResult> resultDataStream =
        builder.append(
            createTriggerStream(),
            tableName,
            builder.maintenanceTaskName(),
            DEFAULT_TASK_INDEX,
            tableLoader,
            DEFAULT_UID_SUFFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            ExecutionConfig.PARALLELISM_DEFAULT);
    try (CloseableIterator<TaskResult> iter = resultDataStream.executeAndCollect()) {
      List<TaskResult> taskResultList = Lists.newArrayList(iter);
      if (taskResultList.isEmpty()) {
        return null;
      }

      return taskResultList.get(0);
    } catch (Exception e) {
      throw new RuntimeException("TableMaintenance error.", e);
    }
  }

  @SuppressWarnings("unchecked")
  public T uidSuffix(String newUidSuffix) {
    builder.uidSuffix(newUidSuffix);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T slotSharingGroup(String newSlotSharingGroup) {
    builder.slotSharingGroup(newSlotSharingGroup);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T parallelism(int parallelism) {
    builder.parallelism(parallelism);
    return (T) this;
  }

  private DataStream<Trigger> createTriggerStream() {
    return env.fromData(Trigger.create(triggerTimestamp, 0))
        .assignTimestampsAndWatermarks(new TableMaintenance.PunctuatedWatermarkStrategy())
        .forceNonParallel();
  }
}

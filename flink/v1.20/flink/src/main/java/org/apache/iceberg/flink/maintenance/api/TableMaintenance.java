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
import java.util.List;
import java.util.UUID;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.LockRemover;
import org.apache.iceberg.flink.maintenance.operator.MonitorSource;
import org.apache.iceberg.flink.maintenance.operator.TableChange;
import org.apache.iceberg.flink.maintenance.operator.TaskResult;
import org.apache.iceberg.flink.maintenance.operator.Trigger;
import org.apache.iceberg.flink.maintenance.operator.TriggerEvaluator;
import org.apache.iceberg.flink.maintenance.operator.TriggerLockFactory;
import org.apache.iceberg.flink.maintenance.operator.TriggerManager;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/** Creates the table maintenance graph. */
public class TableMaintenance {
  static final String SOURCE_OPERATOR_NAME = "Monitor source";
  static final String TRIGGER_MANAGER_OPERATOR_NAME = "Trigger manager";
  static final String WATERMARK_ASSIGNER_OPERATOR_NAME = "Watermark Assigner";
  static final String FILTER_OPERATOR_NAME_PREFIX = "Filter ";
  static final String LOCK_REMOVER_OPERATOR_NAME = "Lock remover";

  private TableMaintenance() {
    // Do not instantiate directly
  }

  /**
   * Use when the change stream is already provided, like in the {@link
   * IcebergSink#addPostCommitTopology(DataStream)}.
   *
   * @param changeStream the table changes
   * @param tableLoader used for accessing the table
   * @param lockFactory used for preventing concurrent task runs
   * @return builder for the maintenance stream
   */
  public static Builder forChangeStream(
      DataStream<TableChange> changeStream,
      TableLoader tableLoader,
      TriggerLockFactory lockFactory) {
    Preconditions.checkNotNull(changeStream, "The change stream should not be null");
    Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");
    Preconditions.checkNotNull(lockFactory, "LockFactory should not be null");

    return new Builder(changeStream, tableLoader, lockFactory);
  }

  /**
   * Use this for standalone maintenance job. It creates a monitor source that detect table changes
   * and build the maintenance pipelines afterwards.
   *
   * @param env used to register the monitor source
   * @param tableLoader used for accessing the table
   * @param lockFactory used for preventing concurrent task runs
   * @return builder for the maintenance stream
   */
  public static Builder forTable(
      StreamExecutionEnvironment env, TableLoader tableLoader, TriggerLockFactory lockFactory) {
    Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");
    Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");
    Preconditions.checkNotNull(lockFactory, "LockFactory should not be null");

    return new Builder(env, tableLoader, lockFactory);
  }

  public static class Builder {
    private final StreamExecutionEnvironment env;
    private final DataStream<TableChange> inputStream;
    private final TableLoader tableLoader;
    private final List<MaintenanceTaskBuilder<?>> taskBuilders;
    private final TriggerLockFactory lockFactory;

    private String uidSuffix = "TableMaintenance-" + UUID.randomUUID();
    private String slotSharingGroup = StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP;
    private Duration rateLimit = Duration.ofMinutes(1);
    private Duration lockCheckDelay = Duration.ofSeconds(30);
    private int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
    private int maxReadBack = 100;

    private Builder(
        StreamExecutionEnvironment env, TableLoader tableLoader, TriggerLockFactory lockFactory) {
      this.env = env;
      this.inputStream = null;
      this.tableLoader = tableLoader;
      this.lockFactory = lockFactory;
      this.taskBuilders = Lists.newArrayListWithCapacity(4);
    }

    private Builder(
        DataStream<TableChange> inputStream,
        TableLoader tableLoader,
        TriggerLockFactory lockFactory) {
      this.env = null;
      this.inputStream = inputStream;
      this.tableLoader = tableLoader;
      this.lockFactory = lockFactory;
      this.taskBuilders = Lists.newArrayListWithCapacity(4);
    }

    /**
     * The prefix used for the generated {@link Transformation}'s uid.
     *
     * @param newUidSuffix for the transformations
     */
    public Builder uidSuffix(String newUidSuffix) {
      this.uidSuffix = newUidSuffix;
      return this;
    }

    /**
     * The {@link SingleOutputStreamOperator#slotSharingGroup(String)} for all the operators of the
     * generated stream. Could be used to separate the resources used by this task.
     *
     * @param newSlotSharingGroup to be used for the operators
     */
    public Builder slotSharingGroup(String newSlotSharingGroup) {
      this.slotSharingGroup = newSlotSharingGroup;
      return this;
    }

    /**
     * Limits the firing frequency for the task triggers.
     *
     * @param newRateLimit firing frequency
     */
    public Builder rateLimit(Duration newRateLimit) {
      Preconditions.checkNotNull(rateLimit.toMillis() > 0, "Rate limit should be greater than 0");
      this.rateLimit = newRateLimit;
      return this;
    }

    /**
     * Sets the delay for checking lock availability when a concurrent run is detected.
     *
     * @param newLockCheckDelay lock checking frequency
     */
    public Builder lockCheckDelay(Duration newLockCheckDelay) {
      this.lockCheckDelay = newLockCheckDelay;
      return this;
    }

    /**
     * Sets the default parallelism of maintenance tasks. Could be overwritten by the {@link
     * MaintenanceTaskBuilder#parallelism(int)}.
     *
     * @param newParallelism task parallelism
     */
    public Builder parallelism(int newParallelism) {
      OperatorValidationUtils.validateParallelism(newParallelism);
      this.parallelism = newParallelism;
      return this;
    }

    /**
     * Maximum number of snapshots checked when started with an embedded {@link MonitorSource} at
     * the first time. Only available when the {@link
     * TableMaintenance#forTable(StreamExecutionEnvironment, TableLoader, TriggerLockFactory)} is
     * used.
     *
     * @param newMaxReadBack snapshots to consider when initializing
     */
    public Builder maxReadBack(int newMaxReadBack) {
      Preconditions.checkArgument(
          inputStream == null, "Can't set maxReadBack when change stream is provided");
      this.maxReadBack = newMaxReadBack;
      return this;
    }

    /**
     * Adds a specific task with the given schedule.
     *
     * @param task to add
     */
    public Builder add(MaintenanceTaskBuilder<?> task) {
      taskBuilders.add(task);
      return this;
    }

    /** Builds the task graph for the maintenance tasks. */
    public void append() {
      Preconditions.checkArgument(!taskBuilders.isEmpty(), "Provide at least one task");
      Preconditions.checkNotNull(uidSuffix, "Uid suffix should no be null");

      List<String> taskNames = Lists.newArrayListWithCapacity(taskBuilders.size());
      List<TriggerEvaluator> evaluators = Lists.newArrayListWithCapacity(taskBuilders.size());
      for (int i = 0; i < taskBuilders.size(); ++i) {
        taskNames.add(nameFor(taskBuilders.get(i), i));
        evaluators.add(taskBuilders.get(i).evaluator());
      }

      DataStream<Trigger> triggers =
          DataStreamUtils.reinterpretAsKeyedStream(changeStream(), unused -> true)
              .process(
                  new TriggerManager(
                      tableLoader,
                      lockFactory,
                      taskNames,
                      evaluators,
                      rateLimit.toMillis(),
                      lockCheckDelay.toMillis()))
              .name(TRIGGER_MANAGER_OPERATOR_NAME)
              .uid(TRIGGER_MANAGER_OPERATOR_NAME + uidSuffix)
              .slotSharingGroup(slotSharingGroup)
              .forceNonParallel()
              .assignTimestampsAndWatermarks(new PunctuatedWatermarkStrategy())
              .name(WATERMARK_ASSIGNER_OPERATOR_NAME)
              .uid(WATERMARK_ASSIGNER_OPERATOR_NAME + uidSuffix)
              .slotSharingGroup(slotSharingGroup)
              .forceNonParallel();

      // Add the specific tasks
      DataStream<TaskResult> unioned = null;
      for (int i = 0; i < taskBuilders.size(); ++i) {
        int finalIndex = i;
        DataStream<Trigger> filtered =
            triggers
                .filter(t -> t.taskId() != null && t.taskId() == finalIndex)
                .name(FILTER_OPERATOR_NAME_PREFIX + i)
                .forceNonParallel()
                .uid(FILTER_OPERATOR_NAME_PREFIX + i + "-" + uidSuffix)
                .slotSharingGroup(slotSharingGroup);
        MaintenanceTaskBuilder<?> builder = taskBuilders.get(i);
        DataStream<TaskResult> result =
            builder.append(
                filtered,
                i,
                taskNames.get(i),
                tableLoader,
                uidSuffix,
                slotSharingGroup,
                parallelism);
        if (unioned == null) {
          unioned = result;
        } else {
          unioned = unioned.union(result);
        }
      }

      // Add the LockRemover to the end
      unioned
          .transform(
              LOCK_REMOVER_OPERATOR_NAME,
              TypeInformation.of(Void.class),
              new LockRemover(lockFactory, taskNames))
          .forceNonParallel()
          .uid("lock-remover-" + uidSuffix)
          .slotSharingGroup(slotSharingGroup);
    }

    private DataStream<TableChange> changeStream() {
      if (inputStream == null) {
        // Create a monitor source to provide the TableChange stream
        MonitorSource source =
            new MonitorSource(
                tableLoader,
                RateLimiterStrategy.perSecond(1.0 / rateLimit.getSeconds()),
                maxReadBack);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), SOURCE_OPERATOR_NAME)
            .uid(SOURCE_OPERATOR_NAME + uidSuffix)
            .slotSharingGroup(slotSharingGroup)
            .forceNonParallel();
      } else {
        return inputStream.global();
      }
    }
  }

  private static String nameFor(MaintenanceTaskBuilder<?> streamBuilder, int taskId) {
    return String.format("%s [%d]", streamBuilder.getClass().getSimpleName(), taskId);
  }

  @Internal
  public static class PunctuatedWatermarkStrategy implements WatermarkStrategy<Trigger> {
    @Override
    public WatermarkGenerator<Trigger> createWatermarkGenerator(
        WatermarkGeneratorSupplier.Context context) {
      return new WatermarkGenerator<>() {
        @Override
        public void onEvent(Trigger event, long eventTimestamp, WatermarkOutput output) {
          output.emitWatermark(new Watermark(event.timestamp()));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
          // No periodic watermarks
        }
      };
    }

    @Override
    public TimestampAssigner<Trigger> createTimestampAssigner(
        TimestampAssignerSupplier.Context context) {
      return (element, unused) -> element.timestamp();
    }
  }
}

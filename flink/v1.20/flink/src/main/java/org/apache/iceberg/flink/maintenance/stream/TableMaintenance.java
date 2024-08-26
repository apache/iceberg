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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.util.Collector;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.LockRemover;
import org.apache.iceberg.flink.maintenance.operator.MonitorSource;
import org.apache.iceberg.flink.maintenance.operator.TableChange;
import org.apache.iceberg.flink.maintenance.operator.TaskResult;
import org.apache.iceberg.flink.maintenance.operator.Trigger;
import org.apache.iceberg.flink.maintenance.operator.TriggerEvaluator;
import org.apache.iceberg.flink.maintenance.operator.TriggerLockFactory;
import org.apache.iceberg.flink.maintenance.operator.TriggerManager;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/** Creates the table maintenance graph. */
public class TableMaintenance {
  private static final String TASK_NAME_FORMAT = "%s [%d]";
  static final String SOURCE_NAME = "Monitor source";
  static final String TRIGGER_MANAGER_TASK_NAME = "Trigger manager";
  static final String LOCK_REMOVER_TASK_NAME = "Lock remover";

  private TableMaintenance() {
    // Do not instantiate directly
  }

  /**
   * Use when the change stream is already provided, like in the {@link
   * org.apache.iceberg.flink.sink.IcebergSink#addPostCommitTopology(DataStream)}.
   *
   * @param changeStream the table changes
   * @param tableLoader used for accessing the table
   * @param lockFactory used for preventing concurrent task runs
   * @return builder for the maintenance stream
   */
  public static Builder builder(
      DataStream<TableChange> changeStream,
      TableLoader tableLoader,
      TriggerLockFactory lockFactory) {
    Preconditions.checkNotNull(changeStream, "The change stream should not be null");
    Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");
    Preconditions.checkNotNull(lockFactory, "LockFactory should not be null");

    return new Builder(changeStream, tableLoader, lockFactory);
  }

  /**
   * Creates the default monitor source for collecting the table changes and returns a builder for
   * the maintenance stream.
   *
   * @param env used to register the monitor source
   * @param tableLoader used for accessing the table
   * @param lockFactory used for preventing concurrent task runs
   * @return builder for the maintenance stream
   */
  public static Builder builder(
      StreamExecutionEnvironment env, TableLoader tableLoader, TriggerLockFactory lockFactory) {
    Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");
    Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");
    Preconditions.checkNotNull(lockFactory, "LockFactory should not be null");

    return new Builder(env, tableLoader, lockFactory);
  }

  public static class Builder {
    private final StreamExecutionEnvironment env;
    private final DataStream<TableChange> changeStream;
    private final TableLoader tableLoader;
    private final List<MaintenanceTaskBuilder<?>> taskBuilders;
    private final TriggerLockFactory lockFactory;

    private String uidPrefix = "TableMaintenance-" + UUID.randomUUID();
    private String slotSharingGroup = StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP;
    private Duration rateLimit = Duration.ofMillis(1);
    private Duration concurrentCheckDelay = Duration.ofSeconds(30);
    private Integer parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
    private int maxReadBack = 100;

    private Builder(
        StreamExecutionEnvironment env, TableLoader tableLoader, TriggerLockFactory lockFactory) {
      this.env = env;
      this.changeStream = null;
      this.tableLoader = tableLoader;
      this.lockFactory = lockFactory;
      this.taskBuilders = Lists.newArrayListWithCapacity(4);
    }

    private Builder(
        DataStream<TableChange> changeStream,
        TableLoader tableLoader,
        TriggerLockFactory lockFactory) {
      this.env = null;
      this.changeStream = changeStream;
      this.tableLoader = tableLoader;
      this.lockFactory = lockFactory;
      this.taskBuilders = Lists.newArrayListWithCapacity(4);
    }

    /**
     * The prefix used for the generated {@link org.apache.flink.api.dag.Transformation}'s uid.
     *
     * @param newUidPrefix for the transformations
     * @return for chained calls
     */
    public Builder uidPrefix(String newUidPrefix) {
      this.uidPrefix = newUidPrefix;
      return this;
    }

    /**
     * The {@link SingleOutputStreamOperator#slotSharingGroup(String)} for all the operators of the
     * generated stream. Could be used to separate the resources used by this task.
     *
     * @param newSlotSharingGroup to be used for the operators
     * @return for chained calls
     */
    public Builder slotSharingGroup(String newSlotSharingGroup) {
      this.slotSharingGroup = newSlotSharingGroup;
      return this;
    }

    /**
     * Limits the firing frequency for the task triggers.
     *
     * @param newRateLimit firing frequency
     * @return for chained calls
     */
    public Builder rateLimit(Duration newRateLimit) {
      Preconditions.checkNotNull(rateLimit.toMillis() > 0, "Rate limit should be greater than 0");
      this.rateLimit = newRateLimit;
      return this;
    }

    /**
     * Sets the delay for checking lock availability when a concurrent run is detected.
     *
     * @param newConcurrentCheckDelay firing frequency
     * @return for chained calls
     */
    public Builder concurrentCheckDelay(Duration newConcurrentCheckDelay) {
      this.concurrentCheckDelay = newConcurrentCheckDelay;
      return this;
    }

    /**
     * Sets the global parallelism of maintenance tasks. Could be overwritten by the {@link
     * MaintenanceTaskBuilder#parallelism(int)}.
     *
     * @param newParallelism task parallelism
     * @return for chained calls
     */
    public Builder parallelism(int newParallelism) {
      this.parallelism = newParallelism;
      return this;
    }

    /**
     * Maximum number of snapshots checked when started with an embedded {@link MonitorSource} at
     * the first time. Only available when the {@link MonitorSource} is generated by the builder.
     *
     * @param newMaxReadBack snapshots to consider when initializing
     * @return for chained calls
     */
    public Builder maxReadBack(int newMaxReadBack) {
      Preconditions.checkArgument(
          changeStream == null, "Can't set maxReadBack when change stream is provided");
      this.maxReadBack = newMaxReadBack;
      return this;
    }

    /**
     * Adds a specific task with the given schedule.
     *
     * @param task to add
     * @return for chained calls
     */
    public Builder add(MaintenanceTaskBuilder<?> task) {
      taskBuilders.add(task);
      return this;
    }

    /** Builds the task graph for the maintenance tasks. */
    public void append() {
      Preconditions.checkArgument(!taskBuilders.isEmpty(), "Provide at least one task");
      Preconditions.checkNotNull(uidPrefix, "Uid prefix should no be null");

      DataStream<TableChange> sourceStream;
      if (changeStream == null) {
        // Create a monitor source to provide the TableChange stream
        MonitorSource source =
            new MonitorSource(
                tableLoader,
                RateLimiterStrategy.perSecond(1.0 / rateLimit.getSeconds()),
                maxReadBack);
        sourceStream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), SOURCE_NAME)
                .uid(uidPrefix + "-monitor-source")
                .slotSharingGroup(slotSharingGroup)
                .forceNonParallel();
      } else {
        sourceStream = changeStream.global();
      }

      // Chain the TriggerManager
      List<String> taskNames = Lists.newArrayListWithCapacity(taskBuilders.size());
      List<TriggerEvaluator> evaluators = Lists.newArrayListWithCapacity(taskBuilders.size());
      for (int i = 0; i < taskBuilders.size(); ++i) {
        taskNames.add(nameFor(taskBuilders.get(i), i));
        evaluators.add(taskBuilders.get(i).evaluator());
      }

      DataStream<Trigger> triggers =
          // Add TriggerManager to schedule the tasks
          DataStreamUtils.reinterpretAsKeyedStream(sourceStream, unused -> true)
              .process(
                  new TriggerManager(
                      tableLoader,
                      lockFactory,
                      taskNames,
                      evaluators,
                      rateLimit.toMillis(),
                      concurrentCheckDelay.toMillis()))
              .name(TRIGGER_MANAGER_TASK_NAME)
              .uid(uidPrefix + "-trigger-manager")
              .slotSharingGroup(slotSharingGroup)
              .forceNonParallel()
              // Add a watermark after every trigger
              .assignTimestampsAndWatermarks(new WindowClosingWatermarkStrategy())
              .name("Watermark Assigner")
              .uid(uidPrefix + "-watermark-assigner")
              .slotSharingGroup(slotSharingGroup)
              .forceNonParallel();

      // Add the specific tasks
      DataStream<TaskResult> unioned = null;
      for (int i = 0; i < taskBuilders.size(); ++i) {
        DataStream<Trigger> filtered =
            triggers
                .flatMap(new TaskFilter(i))
                .name("Filter " + i)
                .forceNonParallel()
                .uid(uidPrefix + "-filter-" + i)
                .slotSharingGroup(slotSharingGroup);
        MaintenanceTaskBuilder<?> builder = taskBuilders.get(i);
        DataStream<TaskResult> result =
            builder.build(
                filtered,
                i,
                taskNames.get(i),
                tableLoader,
                uidPrefix,
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
          .global()
          .transform(
              LOCK_REMOVER_TASK_NAME,
              TypeInformation.of(Void.class),
              new LockRemover(lockFactory, taskNames))
          .forceNonParallel()
          .uid(uidPrefix + "-lock-remover")
          .slotSharingGroup(slotSharingGroup);
    }
  }

  private static String nameFor(MaintenanceTaskBuilder<?> streamBuilder, int taskId) {
    return String.format(TASK_NAME_FORMAT, streamBuilder.getClass().getSimpleName(), taskId);
  }

  @Internal
  public static class WindowClosingWatermarkStrategy implements WatermarkStrategy<Trigger> {
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

  @Internal
  private static class TaskFilter implements FlatMapFunction<Trigger, Trigger> {
    private final int taskId;

    private TaskFilter(int taskId) {
      this.taskId = taskId;
    }

    @Override
    public void flatMap(Trigger trigger, Collector<Trigger> out) {
      if (trigger.taskId() != null && trigger.taskId() == taskId) {
        out.collect(trigger);
      }
    }
  }
}

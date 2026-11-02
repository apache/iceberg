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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.DVWriteResult;
import org.apache.iceberg.flink.maintenance.operator.EqualityConvertCommitter;
import org.apache.iceberg.flink.maintenance.operator.EqualityConvertDVWriter;
import org.apache.iceberg.flink.maintenance.operator.EqualityConvertPKIndex;
import org.apache.iceberg.flink.maintenance.operator.EqualityConvertPlan;
import org.apache.iceberg.flink.maintenance.operator.EqualityConvertPlanner;
import org.apache.iceberg.flink.maintenance.operator.EqualityConvertReader;
import org.apache.iceberg.flink.maintenance.operator.IndexCommand;
import org.apache.iceberg.flink.maintenance.operator.ReadCommand;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.flink.maintenance.operator.TaskResultAggregator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.NestedField;

/**
 * Creates the equality delete to DV conversion data stream. Runs a single iteration of the
 * conversion for every {@link Trigger} event.
 *
 * <p>The pipeline reads equality delete files from a staging branch, converts them to deletion
 * vectors (DVs) using a primary key index stored in Flink state, and commits the data files and DVs
 * to the target branch.
 *
 * <p>The conversion is split into parallel stages:
 *
 * <ol>
 *   <li>Planner (p=1): scans staging branch, emits file-level ReadCommands with phase timestamps
 *   <li>Reader (p=N): reads files, emits row-level IndexCommands
 *   <li>PKIndex (p=N): maintains PK index shards, resolves equality deletes to DV positions
 *   <li>DVWriter (p=N, keyed by data file path): buffers positions per file, writes Puffin DVs
 *       inline
 *   <li>Committer (p=1): commits data files and DVs to the target branch
 * </ol>
 *
 * <p>Mutual exclusion with concurrent maintenance tasks (e.g. compaction) is enforced by the Flink
 * maintenance framework lock.
 */
@Experimental
public class ConvertEqualityDeletes {
  static final String PLANNER_TASK_NAME = "EqConvert Planner";
  static final String READER_TASK_NAME = "EqConvert Reader";
  static final String PK_INDEX_TASK_NAME = "EqConvert PKIndex";
  static final String DV_WRITER_TASK_NAME = "EqConvert DVWriter";
  static final String UPSTREAM_ABORT_TASK_NAME = "EqConvert UpstreamAbort";
  static final String COMMIT_TASK_NAME = "EqConvert Commit";
  static final String AGGREGATOR_TASK_NAME = "EqConvert Aggregator";

  private ConvertEqualityDeletes() {}

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends MaintenanceTaskBuilder<Builder> {
    private String stagingBranch;
    private String targetBranch = SnapshotRef.MAIN_BRANCH;
    private List<String> equalityFieldColumns = Collections.emptyList();

    @Override
    String maintenanceTaskName() {
      return "ConvertEqualityDeletes";
    }

    /** Sets the staging branch name that holds the equality delete files and data files. */
    public Builder stagingBranch(String newStagingBranch) {
      this.stagingBranch = newStagingBranch;
      return this;
    }

    /**
     * Sets the target branch where converted data files and DVs are committed. Defaults to the main
     * branch.
     */
    public Builder targetBranch(String newTargetBranch) {
      this.targetBranch = newTargetBranch;
      return this;
    }

    /**
     * Sets the equality field columns used by the worker index. Required. Must match the equality
     * field columns the writer uses for staging eq-delete files. Mirrors {@link
     * org.apache.iceberg.flink.sink.IcebergSink.Builder#equalityFieldColumns}.
     *
     * <p>The partition source columns of an equality delete's spec must be a subset of these
     * columns. Writes via Flink's IcebergSink already ensure this.
     */
    public Builder equalityFieldColumns(List<String> columns) {
      Preconditions.checkNotNull(columns, "equalityFieldColumns must not be null");
      Preconditions.checkArgument(!columns.isEmpty(), "equalityFieldColumns must not be empty");
      this.equalityFieldColumns = ImmutableList.copyOf(columns);
      return this;
    }

    /**
     * Configures the scheduling of the conversion.
     *
     * @param config properties for the conversion, see {@link ConvertEqualityDeletesConfig} for
     *     available keys
     */
    public Builder config(ConvertEqualityDeletesConfig config) {
      scheduleOnCommitCount(config.scheduleOnCommitCount());
      Long intervalSecond = config.scheduleOnIntervalSecond();
      if (intervalSecond != null) {
        scheduleOnInterval(Duration.ofSeconds(intervalSecond));
      }

      return this;
    }

    @Override
    DataStream<TaskResult> append(DataStream<Trigger> trigger) {
      Preconditions.checkNotNull(stagingBranch, "stagingBranch must be set");
      Preconditions.checkArgument(
          !equalityFieldColumns.isEmpty(), "equalityFieldColumns must be set on the builder");
      Set<Integer> eqFieldIds = resolveEqualityFieldIds();

      // Planner (p=1): emits ReadCommands with phase timestamps and watermarks
      SingleOutputStreamOperator<ReadCommand> planned =
          setSlotSharingGroup(
              trigger
                  .transform(
                      operatorName(PLANNER_TASK_NAME),
                      TypeInformation.of(ReadCommand.class),
                      new EqualityConvertPlanner(
                          tableName(),
                          taskName(),
                          tableLoader(),
                          stagingBranch,
                          targetBranch,
                          eqFieldIds))
                  .uid(PLANNER_TASK_NAME + uidSuffix())
                  .forceNonParallel());

      // Reader (p=N): reads files, emits IndexCommands
      SingleOutputStreamOperator<IndexCommand> index =
          setSlotSharingGroup(
              planned
                  .rebalance()
                  .process(
                      new EqualityConvertReader(
                          tableLoader(), eqFieldIds, stagingBranch.equals(targetBranch)))
                  .name(operatorName(READER_TASK_NAME))
                  .uid(READER_TASK_NAME + uidSuffix())
                  .setParallelism(parallelism()));

      // Broadcast from the planner to the PKIndex to clear the entire index
      BroadcastStream<IndexCommand> clearIndexBroadcast =
          planned
              .getSideOutput(EqualityConvertPlanner.CLEAR_BROADCAST_STREAM)
              .broadcast(EqualityConvertPKIndex.CLEAR_BROADCAST_DESCRIPTOR);

      // PKIndex (p=N): keyed by full PK, phase-aware buffering.
      SingleOutputStreamOperator<DVPosition> dvPositions =
          setSlotSharingGroup(
              index
                  .keyBy(IndexCommand::key, TypeInformation.of(SerializedEqualityValues.class))
                  .connect(clearIndexBroadcast)
                  .process(new EqualityConvertPKIndex(stagingBranch.equals(targetBranch)))
                  .name(operatorName(PK_INDEX_TASK_NAME))
                  .uid(PK_INDEX_TASK_NAME + uidSuffix())
                  .setParallelism(parallelism()));

      // Reader-side abort signals bypass the PKIndex and feed the DVWriter directly, so a reader
      // failure can short-circuit the cycle without waiting on a keyed shuffle. This is not a full
      // short-circuit: the abort is keyed by data file path (empty for ABORT), so only one resolver
      // subtask observes it; the others still write their buffered DVs, which the committer then
      // drops.
      DataStream<DVPosition> readerAborts =
          index.getSideOutput(EqualityConvertReader.READER_ABORT_STREAM);
      DataStream<DVPosition> dvPositionsWithAborts = dvPositions.union(readerAborts);

      // Metadata side output from planner
      DataStream<EqualityConvertPlan> metadata =
          planned.getSideOutput(EqualityConvertPlanner.METADATA_STREAM);

      // DVWriter (p=N, keyed by data file path): groups positions per file, writes Puffin DV
      // files inline, emits a DVWriteResult per cycle. Plan metadata broadcast so every subtask
      // sees it.
      SingleOutputStreamOperator<DVWriteResult> resolved =
          setSlotSharingGroup(
              dvPositionsWithAborts
                  .keyBy(DVPosition::dataFilePath)
                  .connect(metadata.broadcast())
                  .transform(
                      operatorName(DV_WRITER_TASK_NAME),
                      TypeInformation.of(DVWriteResult.class),
                      new EqualityConvertDVWriter(
                          tableName(), taskName(), tableLoader(), targetBranch))
                  .uid(DV_WRITER_TASK_NAME + uidSuffix())
                  .setParallelism(parallelism()));

      // Upstream errors become abort signals so a partial read never commits. The same error side
      // outputs also feed the aggregator below to surface the exception in TaskResult; the two
      // consumers serve different purposes and must both exist.
      DataStream<DVWriteResult> upstreamAborts =
          setSlotSharingGroup(
              index
                  .getSideOutput(TaskResultAggregator.ERROR_STREAM)
                  .union(dvPositions.getSideOutput(TaskResultAggregator.ERROR_STREAM))
                  .map(e -> DVWriteResult.ABORT)
                  .returns(TypeInformation.of(DVWriteResult.class))
                  .name(operatorName(UPSTREAM_ABORT_TASK_NAME))
                  .uid(UPSTREAM_ABORT_TASK_NAME + uidSuffix())
                  .forceNonParallel());

      // Committer (p=1): commits data files + DVs to main.
      SingleOutputStreamOperator<Trigger> committed =
          setSlotSharingGroup(
              resolved
                  .union(upstreamAborts)
                  .connect(metadata)
                  .transform(
                      operatorName(COMMIT_TASK_NAME),
                      TypeInformation.of(Trigger.class),
                      new EqualityConvertCommitter(
                          tableName(), taskName(), tableLoader(), stagingBranch, targetBranch))
                  .uid(COMMIT_TASK_NAME + uidSuffix())
                  .forceNonParallel());

      // Aggregator (p=1): collects errors and emits TaskResult.
      return setSlotSharingGroup(
          committed
              .connect(
                  planned
                      .getSideOutput(TaskResultAggregator.ERROR_STREAM)
                      .union(index.getSideOutput(TaskResultAggregator.ERROR_STREAM))
                      .union(dvPositions.getSideOutput(TaskResultAggregator.ERROR_STREAM))
                      .union(resolved.getSideOutput(TaskResultAggregator.ERROR_STREAM))
                      .union(committed.getSideOutput(TaskResultAggregator.ERROR_STREAM)))
              .transform(
                  operatorName(AGGREGATOR_TASK_NAME),
                  TypeInformation.of(TaskResult.class),
                  new TaskResultAggregator(tableName(), taskName(), index()))
              .uid(AGGREGATOR_TASK_NAME + uidSuffix())
              .forceNonParallel());
    }

    private Set<Integer> resolveEqualityFieldIds() {
      if (!tableLoader().isOpen()) {
        tableLoader().open();
      }

      Table table = tableLoader().loadTable();
      int formatVersion = TableUtil.formatVersion(table);
      Preconditions.checkArgument(
          formatVersion >= 3,
          "ConvertEqualityDeletes requires table format version >= 3 (DVs), "
              + "but table '%s' is version %s",
          tableName(),
          formatVersion);

      Schema schema = table.schema();
      List<Integer> fieldIds = Lists.newArrayListWithCapacity(equalityFieldColumns.size());
      for (String column : equalityFieldColumns) {
        NestedField field = schema.findField(column);
        Preconditions.checkArgument(
            field != null,
            "Equality field column '%s' not found in table schema %s",
            column,
            schema);
        fieldIds.add(field.fieldId());
      }

      return ImmutableSet.copyOf(fieldIds);
    }
  }
}

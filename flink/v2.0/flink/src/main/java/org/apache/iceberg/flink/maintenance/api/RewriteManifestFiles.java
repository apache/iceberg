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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.maintenance.operator.AllManifests;
import org.apache.iceberg.flink.maintenance.operator.FilterSplitsByFileName;
import org.apache.iceberg.flink.maintenance.operator.IncompatibleChangeBlocker;
import org.apache.iceberg.flink.maintenance.operator.ManifestCommitter;
import org.apache.iceberg.flink.maintenance.operator.ManifestEntityReader;
import org.apache.iceberg.flink.maintenance.operator.MetadataTablePlanner;
import org.apache.iceberg.flink.maintenance.operator.TaskResultAggregator;
import org.apache.iceberg.flink.maintenance.operator.WriteManifests;
import org.apache.iceberg.flink.source.ScanContext;

public class RewriteManifestFiles {

  static final String MANIFEST_SCAN_NAME = "Manifest Scan";
  static final String SPEC_CHANGE_NAME = "Spec change blocker";
  static final String PLANNER_TASK_NAME = "Manifest Table Planner";
  static final String FILTER_TASK_NAME = "Filter Table Planner";
  static final String READER_TASK_NAME = "Manifest Table Reader";
  static final String GROUP_TASK_NAME = "Manifest Table GROUP";
  static final String WRITE_TASK_NAME = "Write Task";
  static final String COMMIT_TASK_NAME = "Rewrite Manifest Commit";
  static final String AGGREGATOR_TASK_NAME = "Rewrite Manifest Aggregator";

  private RewriteManifestFiles() {}

  /** Creates the builder for a stream which rewrites data files for the table. */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends MaintenanceTaskBuilder<Builder> {

    private int planningWorkerPoolSize = 10;
    private Long targetManifestSizeBytes = null;
    private Integer rowsDivisor = null;
    private boolean failOnSchemaChange;

    @Override
    String maintenanceTaskName() {
      return "RewriteManifestFiles";
    }

    /**
     * Sets the target manifest file size if it is different from the table default defined by
     * {@link TableProperties#MANIFEST_TARGET_SIZE_BYTES}.
     *
     * @param newTargetManifestSizeBytes target file size
     * @return for chained calls
     */
    public Builder targetManifestSizeBytes(long newTargetManifestSizeBytes) {
      this.targetManifestSizeBytes = newTargetManifestSizeBytes;
      return this;
    }

    /**
     * Sets the divisor for the number of rows used in manifest file planning. This value is used to
     * adjust the target number of rows per manifest file.
     *
     * @param newRowsDivisor the divisor for the number of rows
     * @return this builder for method chaining
     */
    public Builder rowsDivisor(int newRowsDivisor) {
      this.rowsDivisor = newRowsDivisor;
      return this;
    }

    /**
     * The worker pool size used for planning the scan of the {@link MetadataTableType#ALL_FILES}
     * table. This scan is used for determining the files used by the table.
     *
     * @param newPlanningWorkerPoolSize for scanning
     * @return for chained calls
     */
    public Builder planningWorkerPoolSize(int newPlanningWorkerPoolSize) {
      this.planningWorkerPoolSize = newPlanningWorkerPoolSize;
      return this;
    }

    /**
     * If there is a spec change on the table, then the state of the manifest rewrite task becomes
     * invalid. If failOnSpecChange is set to <code>true</code>, then the job will stop with {@link
     * org.apache.flink.runtime.execution.SuppressRestartsException} to prevent job restarts. If
     * failOnSpecChange is set to <code>false</code>, then the job will continue to run but the
     * manifest rewrite job will stop working.
     *
     * @param newFailOnSpecChange to stop the job on table spec change
     * @return for chained calls
     */
    public Builder failOnSpecChange(boolean newFailOnSpecChange) {
      this.failOnSchemaChange = newFailOnSpecChange;
      return this;
    }

    @Override
    DataStream<TaskResult> append(DataStream<Trigger> trigger) {

      SingleOutputStreamOperator<Trigger> checked =
          trigger
              .transform(
                  operatorName(SPEC_CHANGE_NAME),
                  TypeInformation.of(Trigger.class),
                  new IncompatibleChangeBlocker(
                      taskName(), index(), tableLoader(), failOnSchemaChange))
              .name(operatorName(SPEC_CHANGE_NAME))
              .uid(SPEC_CHANGE_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      SingleOutputStreamOperator<ManifestFile> manifestFileDataStream =
          trigger
              .process(new AllManifests(tableLoader(), taskName(), index()))
              .name(operatorName(MANIFEST_SCAN_NAME))
              .uid(MANIFEST_SCAN_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      SingleOutputStreamOperator<MetadataTablePlanner.SplitInfo> splits =
          trigger
              .process(
                  new MetadataTablePlanner(
                      taskName(),
                      index(),
                      tableLoader(),
                      ScanContext.builder().streaming(true).build(),
                      MetadataTableType.ENTRIES,
                      planningWorkerPoolSize))
              .name(operatorName(PLANNER_TASK_NAME))
              .uid(PLANNER_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      SingleOutputStreamOperator<MetadataTablePlanner.SplitInfo> filteredSplits =
          manifestFileDataStream
              .connect(splits)
              .transform(
                  operatorName(FILTER_TASK_NAME),
                  TypeInformation.of(MetadataTablePlanner.SplitInfo.class),
                  new FilterSplitsByFileName())
              .name(operatorName(FILTER_TASK_NAME))
              .uid(FILTER_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      SingleOutputStreamOperator<RowData> manifestInfoDataStream =
          filteredSplits
              .rebalance()
              .process(
                  new ManifestEntityReader(
                      taskName(), index(), tableLoader(), MetadataTableType.ENTRIES))
              .name(operatorName(READER_TASK_NAME))
              .uid(READER_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      WriteManifests writeManifests =
          new WriteManifests(
              taskName(), index(), tableLoader(), targetManifestSizeBytes, rowsDivisor);
      SingleOutputStreamOperator<ManifestFile> writeManifest =
          writeManifest(manifestInfoDataStream, writeManifests);

      DataStream<Exception> exceptions =
          manifestInfoDataStream
              .getSideOutput(TaskResultAggregator.ERROR_STREAM)
              .union(
                  manifestFileDataStream.getSideOutput(TaskResultAggregator.ERROR_STREAM),
                  checked.getSideOutput(TaskResultAggregator.ERROR_STREAM),
                  writeManifest.getSideOutput(TaskResultAggregator.ERROR_STREAM),
                  filteredSplits.getSideOutput(TaskResultAggregator.ERROR_STREAM));

      SingleOutputStreamOperator<Trigger> updated =
          manifestFileDataStream
              .map(
                  (MapFunction<
                          ManifestFile, Tuple2<ManifestFile, ManifestCommitter.ManifestSource>>)
                      value -> Tuple2.of(value, ManifestCommitter.ManifestSource.OLD))
              .name(operatorName(GROUP_TASK_NAME + "_old"))
              .uid(GROUP_TASK_NAME + "_old" + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .returns(TypeInformation.of(new TypeHint<>() {}))
              .union(
                  writeManifest
                      .map(
                          (MapFunction<
                                  ManifestFile,
                                  Tuple2<ManifestFile, ManifestCommitter.ManifestSource>>)
                              value -> Tuple2.of(value, ManifestCommitter.ManifestSource.NEW))
                      .name(operatorName(GROUP_TASK_NAME + "_new"))
                      .uid(GROUP_TASK_NAME + "_new" + uidSuffix())
                      .slotSharingGroup(slotSharingGroup())
                      .returns(TypeInformation.of(new TypeHint<>() {})))
              .connect(exceptions)
              .transform(
                  operatorName(COMMIT_TASK_NAME),
                  TypeInformation.of(Trigger.class),
                  new ManifestCommitter(taskName(), index(), tableLoader()))
              .uid(COMMIT_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      return trigger
          .union(checked)
          .union(updated)
          .connect(exceptions.union(updated.getSideOutput(TaskResultAggregator.ERROR_STREAM)))
          .transform(
              operatorName(AGGREGATOR_TASK_NAME),
              TypeInformation.of(TaskResult.class),
              new TaskResultAggregator(tableName(), taskName(), index()))
          .uid(AGGREGATOR_TASK_NAME + uidSuffix())
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel();
    }

    private SingleOutputStreamOperator<ManifestFile> writeManifest(
        SingleOutputStreamOperator<RowData> entries, WriteManifests writer) {
      return entries
          .transform(WRITE_TASK_NAME, TypeInformation.of(ManifestFile.class), writer)
          .name(operatorName(WRITE_TASK_NAME))
          .uid(WRITE_TASK_NAME + uidSuffix())
          .slotSharingGroup(slotSharingGroup())
          .setParallelism(parallelism());
    }
  }
}

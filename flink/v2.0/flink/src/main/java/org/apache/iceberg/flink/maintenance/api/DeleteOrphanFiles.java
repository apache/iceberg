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
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles.PrefixMismatchMode;
import org.apache.iceberg.flink.maintenance.operator.DeleteFilesProcessor;
import org.apache.iceberg.flink.maintenance.operator.FileNameReader;
import org.apache.iceberg.flink.maintenance.operator.FileUriKeySelector;
import org.apache.iceberg.flink.maintenance.operator.ListFileSystemFiles;
import org.apache.iceberg.flink.maintenance.operator.ListMetadataFiles;
import org.apache.iceberg.flink.maintenance.operator.MetadataTablePlanner;
import org.apache.iceberg.flink.maintenance.operator.OrphanFilesDetector;
import org.apache.iceberg.flink.maintenance.operator.SkipOnError;
import org.apache.iceberg.flink.maintenance.operator.TaskResultAggregator;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ThreadPools;

/** Delete orphan files from the file system. */
public class DeleteOrphanFiles {

  private static final Schema FILE_PATH_SCHEMA = new Schema(DataFile.FILE_PATH);
  private static final ScanContext FILE_PATH_SCAN_CONTEXT =
      ScanContext.builder().streaming(true).project(FILE_PATH_SCHEMA).build();
  private static final Splitter COMMA_SPLITTER = Splitter.on(",");

  @Internal
  public static final OutputTag<Exception> ERROR_STREAM =
      new OutputTag<>("error-stream", TypeInformation.of(Exception.class));

  static final String PLANNER_TASK_NAME = "Table Planner";
  static final String READER_TASK_NAME = "Files Reader";
  static final String FILESYSTEM_FILES_TASK_NAME = "Filesystem Files";
  static final String METADATA_FILES_TASK_NAME = "List metadata Files";
  static final String DELETE_FILES_TASK_NAME = "Delete File";
  static final String AGGREGATOR_TASK_NAME = "Orphan Files Aggregator";
  static final String FILTER_FILES_TASK_NAME = "Filter File";
  static final String SKIP_ON_ERROR_TASK_NAME = "Skip On Error";

  public static DeleteOrphanFiles.Builder builder() {
    return new DeleteOrphanFiles.Builder();
  }

  private DeleteOrphanFiles() {
    // Do not instantiate directly
  }

  public static class Builder extends MaintenanceTaskBuilder<DeleteOrphanFiles.Builder> {
    private String location;
    private Duration minAge = Duration.ofDays(3);
    private int planningWorkerPoolSize = ThreadPools.WORKER_THREAD_POOL_SIZE;
    private int deleteBatchSize = 1000;
    private boolean usePrefixListing = false;
    private Map<String, String> equalSchemes =
        Maps.newHashMap(
            ImmutableMap.of(
                "s3n", "s3",
                "s3a", "s3"));
    private final Map<String, String> equalAuthorities = Maps.newHashMap();
    private PrefixMismatchMode prefixMismatchMode = PrefixMismatchMode.ERROR;

    @Override
    String maintenanceTaskName() {
      return "DeleteOrphanFiles";
    }

    /**
     * The location to start the recursive listing the candidate files for removal. By default, the
     * {@link Table#location()} is used.
     *
     * @param newLocation the task will scan
     * @return for chained calls
     */
    public Builder location(String newLocation) {
      this.location = newLocation;
      return this;
    }

    /**
     * Whether to use prefix listing when listing files from the file system.
     *
     * @param newUsePrefixListing true to enable prefix listing, false otherwise
     * @return for chained calls
     */
    public Builder usePrefixListing(boolean newUsePrefixListing) {
      this.usePrefixListing = newUsePrefixListing;
      return this;
    }

    /**
     * Action behavior when location prefixes (schemes/authorities) mismatch.
     *
     * @param newPrefixMismatchMode to action when mismatch
     * @return for chained calls
     */
    public Builder prefixMismatchMode(PrefixMismatchMode newPrefixMismatchMode) {
      this.prefixMismatchMode = newPrefixMismatchMode;
      return this;
    }

    /**
     * The files newer than this age will not be removed.
     *
     * @param newMinAge of the files to be removed
     * @return for chained calls
     */
    public Builder minAge(Duration newMinAge) {
      this.minAge = newMinAge;
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
     * Passes schemes that should be considered equal.
     *
     * <p>The key may include a comma-separated list of schemes. For instance,
     * Map("s3a,s3,s3n","s3").
     *
     * @param newEqualSchemes list of equal schemes
     * @return this for method chaining
     */
    public Builder equalSchemes(Map<String, String> newEqualSchemes) {
      equalSchemes.putAll(flattenMap(newEqualSchemes));
      return this;
    }

    /**
     * Passes authorities that should be considered equal.
     *
     * <p>The key may include a comma-separate list of authorities. For instance,
     * Map("s1name,s2name","servicename").
     *
     * @param newEqualAuthorities list of equal authorities
     * @return this for method chaining
     */
    public Builder equalAuthorities(Map<String, String> newEqualAuthorities) {
      equalAuthorities.putAll(flattenMap(newEqualAuthorities));
      return this;
    }

    /**
     * Size of the batch used to deleting the files.
     *
     * @param newDeleteBatchSize number of batch file
     * @return for chained calls
     */
    public Builder deleteBatchSize(int newDeleteBatchSize) {
      this.deleteBatchSize = newDeleteBatchSize;
      return this;
    }

    @Override
    DataStream<TaskResult> append(DataStream<Trigger> trigger) {
      tableLoader().open();

      // Collect all data files
      SingleOutputStreamOperator<MetadataTablePlanner.SplitInfo> splits =
          trigger
              .process(
                  new MetadataTablePlanner(
                      taskName(),
                      index(),
                      tableLoader(),
                      FILE_PATH_SCAN_CONTEXT,
                      MetadataTableType.ALL_FILES,
                      planningWorkerPoolSize))
              .name(operatorName(PLANNER_TASK_NAME))
              .uid(PLANNER_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      // Read the records and get all data files
      SingleOutputStreamOperator<String> tableDataFiles =
          splits
              .rebalance()
              .process(
                  new FileNameReader(
                      taskName(),
                      index(),
                      tableLoader(),
                      FILE_PATH_SCHEMA,
                      FILE_PATH_SCAN_CONTEXT,
                      MetadataTableType.ALL_FILES))
              .name(operatorName(READER_TASK_NAME))
              .uid(READER_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      // Collect all meta data files
      SingleOutputStreamOperator<String> tableMetadataFiles =
          trigger
              .process(new ListMetadataFiles(taskName(), index(), tableLoader()))
              .name(operatorName(METADATA_FILES_TASK_NAME))
              .uid(METADATA_FILES_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      // List the all file system files
      SingleOutputStreamOperator<String> allFsFiles =
          trigger
              .process(
                  new ListFileSystemFiles(
                      taskName(),
                      index(),
                      tableLoader(),
                      location,
                      minAge.toMillis(),
                      usePrefixListing))
              .name(operatorName(FILESYSTEM_FILES_TASK_NAME))
              .uid(FILESYSTEM_FILES_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      SingleOutputStreamOperator<String> filesToDelete =
          tableMetadataFiles
              .union(tableDataFiles)
              .keyBy(new FileUriKeySelector(equalSchemes, equalAuthorities))
              .connect(allFsFiles.keyBy(new FileUriKeySelector(equalSchemes, equalAuthorities)))
              .process(new OrphanFilesDetector(prefixMismatchMode, equalSchemes, equalAuthorities))
              .slotSharingGroup(slotSharingGroup())
              .name(operatorName(FILTER_FILES_TASK_NAME))
              .uid(FILTER_FILES_TASK_NAME + uidSuffix())
              .setParallelism(parallelism());

      DataStream<Exception> errorStream =
          tableMetadataFiles
              .getSideOutput(ERROR_STREAM)
              .union(
                  allFsFiles.getSideOutput(ERROR_STREAM),
                  tableDataFiles.getSideOutput(ERROR_STREAM),
                  splits.getSideOutput(ERROR_STREAM),
                  filesToDelete.getSideOutput(ERROR_STREAM));

      // Stop deleting the files if there is an error
      SingleOutputStreamOperator<String> filesOrSkip =
          filesToDelete
              .connect(errorStream)
              .transform(
                  operatorName(SKIP_ON_ERROR_TASK_NAME),
                  TypeInformation.of(String.class),
                  new SkipOnError())
              .uid(SKIP_ON_ERROR_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      // delete the files
      filesOrSkip
          .rebalance()
          .transform(
              operatorName(DELETE_FILES_TASK_NAME),
              TypeInformation.of(Void.class),
              new DeleteFilesProcessor(
                  tableLoader().loadTable(), taskName(), index(), deleteBatchSize))
          .uid(DELETE_FILES_TASK_NAME + uidSuffix())
          .slotSharingGroup(slotSharingGroup())
          .setParallelism(parallelism());

      // Ignore the file deletion result and return the DataStream<TaskResult> directly
      return trigger
          .connect(errorStream)
          .transform(
              operatorName(AGGREGATOR_TASK_NAME),
              TypeInformation.of(TaskResult.class),
              new TaskResultAggregator(tableName(), taskName(), index()))
          .uid(AGGREGATOR_TASK_NAME + uidSuffix())
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel();
    }
  }

  private static Map<String, String> flattenMap(Map<String, String> map) {
    Map<String, String> flattenedMap = Maps.newHashMap();
    if (map != null) {
      for (String key : map.keySet()) {
        String value = map.get(key);
        for (String splitKey : COMMA_SPLITTER.split(key)) {
          flattenedMap.put(splitKey.trim(), value.trim());
        }
      }
    }

    return flattenedMap;
  }
}

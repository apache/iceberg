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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.actions.ActionsProvider;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.actions.DeleteReachableFiles;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestDeleteReachableFilesAction extends TestBase {
  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));
  private static final int SHUFFLE_PARTITIONS = 2;

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("c1").build();

  static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartition(TestHelpers.Row.of(0))
          .withRecordCount(1)
          .build();
  static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withPartition(TestHelpers.Row.of(1))
          .withRecordCount(1)
          .build();
  static final DataFile FILE_C =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-c.parquet")
          .withFileSizeInBytes(10)
          .withPartition(TestHelpers.Row.of(2))
          .withRecordCount(1)
          .build();
  static final DataFile FILE_D =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-d.parquet")
          .withFileSizeInBytes(10)
          .withPartition(TestHelpers.Row.of(3))
          .withRecordCount(1)
          .build();
  static final DeleteFile FILE_A_POS_DELETES =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath("/path/to/data-a-pos-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartition(TestHelpers.Row.of(0))
          .withRecordCount(1)
          .build();
  static final DeleteFile FILE_A_EQ_DELETES =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofEqualityDeletes()
          .withPath("/path/to/data-a-eq-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartition(TestHelpers.Row.of(0))
          .withRecordCount(1)
          .build();

  @TempDir private Path temp;

  private Table table;

  @BeforeEach
  public void setupTableLocation() throws Exception {
    File tableDir = temp.resolve("junit").toFile();
    String tableLocation = tableDir.toURI().toString();
    this.table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);
    spark.conf().set("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS);
  }

  private void checkRemoveFilesResults(
      long expectedDatafiles,
      long expectedPosDeleteFiles,
      long expectedEqDeleteFiles,
      long expectedManifestsDeleted,
      long expectedManifestListsDeleted,
      long expectedOtherFilesDeleted,
      DeleteReachableFiles.Result results) {

    assertThat(results.deletedManifestsCount())
        .as("Incorrect number of manifest files deleted")
        .isEqualTo(expectedManifestsDeleted);

    assertThat(results.deletedDataFilesCount())
        .as("Incorrect number of datafiles deleted")
        .isEqualTo(expectedDatafiles);

    assertThat(results.deletedPositionDeleteFilesCount())
        .as("Incorrect number of position delete files deleted")
        .isEqualTo(expectedPosDeleteFiles);

    assertThat(results.deletedEqualityDeleteFilesCount())
        .as("Incorrect number of equality delete files deleted")
        .isEqualTo(expectedEqDeleteFiles);

    assertThat(results.deletedManifestListsCount())
        .as("Incorrect number of manifest lists deleted")
        .isEqualTo(expectedManifestListsDeleted);

    assertThat(results.deletedOtherFilesCount())
        .as("Incorrect number of other lists deleted")
        .isEqualTo(expectedOtherFilesDeleted);
  }

  @Test
  public void dataFilesCleanupWithParallelTasks() {
    table.newFastAppend().appendFile(FILE_A).commit();

    table.newFastAppend().appendFile(FILE_B).commit();

    table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_B), ImmutableSet.of(FILE_D)).commit();

    table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_C)).commit();

    Set<String> deletedFiles = ConcurrentHashMap.newKeySet();
    Set<String> deleteThreads = ConcurrentHashMap.newKeySet();
    AtomicInteger deleteThreadsIndex = new AtomicInteger(0);

    DeleteReachableFiles.Result result =
        sparkActions()
            .deleteReachableFiles(metadataLocation(table))
            .io(table.io())
            .executeDeleteWith(
                Executors.newFixedThreadPool(
                    4,
                    runnable -> {
                      Thread thread = new Thread(runnable);
                      thread.setName("remove-files-" + deleteThreadsIndex.getAndIncrement());
                      thread.setDaemon(
                          true); // daemon threads will be terminated abruptly when the JVM exits
                      return thread;
                    }))
            .deleteWith(
                s -> {
                  deleteThreads.add(Thread.currentThread().getName());
                  deletedFiles.add(s);
                })
            .execute();

    // Verifies that the delete methods ran in the threads created by the provided ExecutorService
    // ThreadFactory
    assertThat(deleteThreads)
        .isEqualTo(
            Sets.newHashSet(
                "remove-files-0", "remove-files-1", "remove-files-2", "remove-files-3"));

    Lists.newArrayList(FILE_A, FILE_B, FILE_C, FILE_D)
        .forEach(
            file ->
                assertThat(deletedFiles)
                    .as("FILE_A should be deleted")
                    .contains(FILE_A.path().toString()));
    checkRemoveFilesResults(4L, 0, 0, 6L, 4L, 6, result);
  }

  @Test
  public void testWithExpiringDanglingStageCommit() {
    table.location();
    // `A` commit
    table.newAppend().appendFile(FILE_A).commit();

    // `B` staged commit
    table.newAppend().appendFile(FILE_B).stageOnly().commit();

    // `C` commit
    table.newAppend().appendFile(FILE_C).commit();

    DeleteReachableFiles.Result result =
        sparkActions().deleteReachableFiles(metadataLocation(table)).io(table.io()).execute();

    checkRemoveFilesResults(3L, 0, 0, 3L, 3L, 5, result);
  }

  @Test
  public void testRemoveFileActionOnEmptyTable() {
    DeleteReachableFiles.Result result =
        sparkActions().deleteReachableFiles(metadataLocation(table)).io(table.io()).execute();

    checkRemoveFilesResults(0, 0, 0, 0, 0, 2, result);
  }

  @Test
  public void testRemoveFilesActionWithReducedVersionsTable() {
    table.updateProperties().set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "2").commit();
    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).commit();

    table.newAppend().appendFile(FILE_B).commit();

    table.newAppend().appendFile(FILE_C).commit();

    table.newAppend().appendFile(FILE_D).commit();

    DeleteReachableFiles baseRemoveFilesSparkAction =
        sparkActions().deleteReachableFiles(metadataLocation(table)).io(table.io());
    DeleteReachableFiles.Result result = baseRemoveFilesSparkAction.execute();

    checkRemoveFilesResults(4, 0, 0, 5, 5, 8, result);
  }

  @Test
  public void testRemoveFilesAction() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).commit();

    DeleteReachableFiles baseRemoveFilesSparkAction =
        sparkActions().deleteReachableFiles(metadataLocation(table)).io(table.io());
    checkRemoveFilesResults(2, 0, 0, 2, 2, 4, baseRemoveFilesSparkAction.execute());
  }

  @Test
  public void testPositionDeleteFiles() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).commit();

    table.newRowDelta().addDeletes(FILE_A_POS_DELETES).commit();

    DeleteReachableFiles baseRemoveFilesSparkAction =
        sparkActions().deleteReachableFiles(metadataLocation(table)).io(table.io());
    checkRemoveFilesResults(2, 1, 0, 3, 3, 5, baseRemoveFilesSparkAction.execute());
  }

  @Test
  public void testEqualityDeleteFiles() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).commit();

    table.newRowDelta().addDeletes(FILE_A_EQ_DELETES).commit();

    DeleteReachableFiles baseRemoveFilesSparkAction =
        sparkActions().deleteReachableFiles(metadataLocation(table)).io(table.io());
    checkRemoveFilesResults(2, 0, 1, 3, 3, 5, baseRemoveFilesSparkAction.execute());
  }

  @Test
  public void testRemoveFilesActionWithDefaultIO() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).commit();

    // IO not set explicitly on removeReachableFiles action
    // IO defaults to HadoopFileIO
    DeleteReachableFiles baseRemoveFilesSparkAction =
        sparkActions().deleteReachableFiles(metadataLocation(table));
    checkRemoveFilesResults(2, 0, 0, 2, 2, 4, baseRemoveFilesSparkAction.execute());
  }

  @Test
  public void testUseLocalIterator() {
    table.newFastAppend().appendFile(FILE_A).commit();

    table.newOverwrite().deleteFile(FILE_A).addFile(FILE_B).commit();

    table.newFastAppend().appendFile(FILE_C).commit();

    int jobsBefore = spark.sparkContext().dagScheduler().nextJobId().get();

    withSQLConf(
        ImmutableMap.of("spark.sql.adaptive.enabled", "false"),
        () -> {
          DeleteReachableFiles.Result results =
              sparkActions()
                  .deleteReachableFiles(metadataLocation(table))
                  .io(table.io())
                  .option("stream-results", "true")
                  .execute();

          int jobsAfter = spark.sparkContext().dagScheduler().nextJobId().get();
          int totalJobsRun = jobsAfter - jobsBefore;

          checkRemoveFilesResults(3L, 0, 0, 4L, 3L, 5, results);

          assertThat(totalJobsRun)
              .as("Expected total jobs to be equal to total number of shuffle partitions")
              .isEqualTo(SHUFFLE_PARTITIONS);
        });
  }

  @Test
  public void testIgnoreMetadataFilesNotFound() {
    table.updateProperties().set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1").commit();

    table.newAppend().appendFile(FILE_A).commit();
    // There are three metadata json files at this point
    DeleteOrphanFiles.Result result =
        sparkActions().deleteOrphanFiles(table).olderThan(System.currentTimeMillis()).execute();

    assertThat(result.orphanFileLocations()).as("Should delete 1 file").size().isOne();
    assertThat(StreamSupport.stream(result.orphanFileLocations().spliterator(), false))
        .as("Should remove v1 file")
        .anyMatch(file -> file.contains("v1.metadata.json"));

    DeleteReachableFiles baseRemoveFilesSparkAction =
        sparkActions().deleteReachableFiles(metadataLocation(table)).io(table.io());
    DeleteReachableFiles.Result res = baseRemoveFilesSparkAction.execute();

    checkRemoveFilesResults(1, 0, 0, 1, 1, 4, res);
  }

  @Test
  public void testEmptyIOThrowsException() {
    DeleteReachableFiles baseRemoveFilesSparkAction =
        sparkActions().deleteReachableFiles(metadataLocation(table)).io(null);

    assertThatThrownBy(baseRemoveFilesSparkAction::execute)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("File IO cannot be null");
  }

  @Test
  public void testRemoveFilesActionWhenGarbageCollectionDisabled() {
    table.updateProperties().set(TableProperties.GC_ENABLED, "false").commit();

    assertThatThrownBy(() -> sparkActions().deleteReachableFiles(metadataLocation(table)).execute())
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Cannot delete files: GC is disabled (deleting files may corrupt other tables)");
  }

  private String metadataLocation(Table tbl) {
    return ((HasTableOperations) tbl).operations().current().metadataFileLocation();
  }

  private ActionsProvider sparkActions() {
    return SparkActions.get();
  }
}

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

package org.apache.iceberg.actions;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.actions.BaseRemoveFilesSparkAction;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public abstract class TestRemoveFilesAction extends SparkTestBase {
  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2", Types.StringType.get()),
      optional(3, "c3", Types.StringType.get())
  );
  private static final int SHUFFLE_PARTITIONS = 2;

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("c1").build();

  static final DataFile FILE_A = DataFiles.builder(SPEC)
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("c1=0") // easy way to set partition data for now
      .withRecordCount(1)
      .build();
  static final DataFile FILE_B = DataFiles.builder(SPEC)
      .withPath("/path/to/data-b.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("c1=1") // easy way to set partition data for now
      .withRecordCount(1)
      .build();
  static final DataFile FILE_C = DataFiles.builder(SPEC)
      .withPath("/path/to/data-c.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("c1=2") // easy way to set partition data for now
      .withRecordCount(1)
      .build();
  static final DataFile FILE_D = DataFiles.builder(SPEC)
      .withPath("/path/to/data-d.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("c1=3") // easy way to set partition data for now
      .withRecordCount(1)
      .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Table table;

  @Before
  public void setupTableLocation() throws Exception {
    File tableDir = temp.newFolder();
    String tableLocation = tableDir.toURI().toString();
    this.table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);
    spark.conf().set("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS);
  }

  private void checkRemoveFilesResults(long expectedDatafiles, long expectedManifestsDeleted,
      long expectedManifestListsDeleted, long expectedOtherFilesDeleted, RemoveFiles.Result results) {
    Assert.assertEquals("Incorrect number of manifest files deleted",
        (Long) expectedManifestsDeleted, (Long) results.deletedManifestsCount());
    Assert.assertEquals("Incorrect number of datafiles deleted",
        (Long) expectedDatafiles, (Long) results.deletedDataFilesCount());
    Assert.assertEquals("Incorrect number of manifest lists deleted",
        (Long) expectedManifestListsDeleted, (Long) results.deletedManifestListsCount());
    Assert.assertEquals("Incorrect number of other lists deleted",
        (Long) expectedOtherFilesDeleted, (Long) results.otherDeletedFilesCount());
  }

  @Test
  public void dataFilesCleanupWithParallelTasks() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();

    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();

    table.newRewrite()
        .rewriteFiles(ImmutableSet.of(FILE_B), ImmutableSet.of(FILE_D))
        .commit();

    table.newRewrite()
        .rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_C))
        .commit();


    Set<String> deletedFiles = Sets.newHashSet();
    Set<String> deleteThreads = ConcurrentHashMap.newKeySet();
    AtomicInteger deleteThreadsIndex = new AtomicInteger(0);

    RemoveFiles.Result result = Actions.forTable(table).removeFilesAction()
        .executeDeleteWith(Executors.newFixedThreadPool(4, runnable -> {
          Thread thread = new Thread(runnable);
          thread.setName("remove-files-" + deleteThreadsIndex.getAndIncrement());
          thread.setDaemon(true); // daemon threads will be terminated abruptly when the JVM exits
          return thread;
        }))
        .deleteWith(s -> {
          deleteThreads.add(Thread.currentThread().getName());
          deletedFiles.add(s);
        })
        .execute();

    // Verifies that the delete methods ran in the threads created by the provided ExecutorService ThreadFactory
    Assert.assertEquals(deleteThreads,
        Sets.newHashSet("remove-files-0", "remove-files-1", "remove-files-2", "remove-files-3"));

    Assert.assertTrue("FILE_A should be deleted", deletedFiles.contains(FILE_A.path().toString()));
    Assert.assertTrue("FILE_B should be deleted", deletedFiles.contains(FILE_B.path().toString()));

    checkRemoveFilesResults(4L, 6L, 4L,
        6, result);
  }

  @Test
  public void testWithExpiringDanglingStageCommit() {
    table.location();
    // `A` commit
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    // `B` staged commit
    table.newAppend()
        .appendFile(FILE_B)
        .stageOnly()
        .commit();

    // `C` commit
    table.newAppend()
        .appendFile(FILE_C)
        .commit();


    RemoveFiles.Result result = Actions.forTable(table).removeFilesAction()
        .execute();

    checkRemoveFilesResults(3L, 3L, 3L, 5, result);
  }

  @Test
  public void testRemoveFileActionOnEmptyTable() {
    RemoveFiles.Result result = Actions.forTable(table).removeFilesAction()
        .execute();

    checkRemoveFilesResults(0, 0, 0, 2, result);
  }

  @Test
  public void testRemoveFilesActionWithReducedVersionsTable() {
    table.updateProperties()
        .set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "2").commit();
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    table.newAppend()
        .appendFile(FILE_B)
        .commit();

    table.newAppend()
        .appendFile(FILE_B)
        .commit();

    table.newAppend()
        .appendFile(FILE_C)
        .commit();

    table.newAppend()
        .appendFile(FILE_D)
        .commit();

    BaseRemoveFilesSparkAction baseRemoveFilesSparkAction = Actions.forTable(table)
        .removeFilesAction();
    RemoveFiles.Result result = baseRemoveFilesSparkAction.execute();

    checkRemoveFilesResults(4, 5, 5, 8, result);
  }

  @Test
  public void testRemoveFilesAction() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    table.newAppend()
        .appendFile(FILE_B)
        .commit();

    BaseRemoveFilesSparkAction baseRemoveFilesSparkAction = Actions.forTable(table)
        .removeFilesAction();
    RemoveFiles.Result result = baseRemoveFilesSparkAction.execute();

    checkRemoveFilesResults(2, 2, 2, 4,  result);
  }

  @Test
  public void testUseLocalIterator() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();

    table.newOverwrite()
        .deleteFile(FILE_A)
        .addFile(FILE_B)
        .commit();

    table.newFastAppend()
        .appendFile(FILE_C)
        .commit();

    int jobsBefore = spark.sparkContext().dagScheduler().nextJobId().get();

    RemoveFiles.Result results = Actions.forTable(table).removeFilesAction().option("stream-results", "true").execute();

    int jobsAfter = spark.sparkContext().dagScheduler().nextJobId().get();
    int totalJobsRun = jobsAfter - jobsBefore;

    checkRemoveFilesResults(3L, 4L, 3L, 5, results);

    Assert.assertEquals(
        "Expected total jobs to be equal to total number of shuffle partitions", totalJobsRun, SHUFFLE_PARTITIONS);
  }

  @Test
  public void testIgnoreMetadataFilesNotFound() {
    table.updateProperties()
        .set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1").commit();

    table.newAppend()
        .appendFile(FILE_A)
        .commit();
    // There are three metadata json files at this point
    List<String> result = Actions.forTable(table)
        .removeOrphanFiles()
        .olderThan(System.currentTimeMillis()).execute();

    Assert.assertEquals("Should delete 1 file", 1, result.size());
    Assert.assertTrue("Should remove v1 file", result.get(0).contains("v1.metadata.json"));

    BaseRemoveFilesSparkAction baseRemoveFilesSparkAction = Actions.forTable(table)
        .removeFilesAction();
    RemoveFiles.Result res = baseRemoveFilesSparkAction.execute();

    checkRemoveFilesResults(1, 1, 1, 4,  res);

  }
}

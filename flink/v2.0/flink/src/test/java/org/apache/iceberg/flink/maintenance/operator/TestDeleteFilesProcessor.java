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
package org.apache.iceberg.flink.maintenance.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestDeleteFilesProcessor extends OperatorTestBase {
  private static final String DUMMY_FILE_NAME = "dummy";
  private static final Set<String> TABLE_FILES =
      ImmutableSet.of(
          "metadata/v1.metadata.json",
          "metadata/version-hint.text",
          "metadata/.version-hint.text.crc",
          "metadata/.v1.metadata.json.crc");

  private Table table;

  @BeforeEach
  void before() {
    this.table = createTable();
  }

  @Test
  void testDelete() throws Exception {
    // Write an extra file
    Path dummyFile = Path.of(tablePath(table).toString(), DUMMY_FILE_NAME);
    Files.write(dummyFile, "DUMMY".getBytes(StandardCharsets.UTF_8));

    Set<String> files = listFiles(table);
    assertThat(files)
        .containsAll(TABLE_FILES)
        .contains(DUMMY_FILE_NAME)
        .hasSize(TABLE_FILES.size() + 1);

    deleteFile(tableLoader(), dummyFile.toString());

    assertThat(listFiles(table)).isEqualTo(TABLE_FILES);
  }

  @Test
  void testDeleteMissingFile() throws Exception {
    Path dummyFile =
        FileSystems.getDefault().getPath(table.location().substring(5), DUMMY_FILE_NAME);

    deleteFile(tableLoader(), dummyFile.toString());

    assertThat(listFiles(table)).isEqualTo(TABLE_FILES);
  }

  @Test
  void testInvalidURIScheme() throws Exception {
    deleteFile(tableLoader(), "wrong://");

    assertThat(listFiles(table)).isEqualTo(TABLE_FILES);
  }

  @Test
  void testDeleteNonExistentFile() throws Exception {
    String nonexistentFile = "nonexistentFile.txt";

    deleteFile(tableLoader(), nonexistentFile);

    assertThat(listFiles(table)).isEqualTo(TABLE_FILES);
  }

  @Test
  void testDeleteLargeFile() throws Exception {
    // Simulate a large file (e.g., 100MB file)
    String largeFileName = "largeFile.txt";
    Path largeFile = Path.of(tablePath(table).toString(), largeFileName);

    // Write a large file to disk (this will simulate the large file in the filesystem)
    byte[] largeData = new byte[1024 * 1024 * 100]; // 100 MB
    Files.write(largeFile, largeData);

    // Verify that the file was created
    Set<String> files = listFiles(table);
    assertThat(files).contains(largeFileName);

    // Use the DeleteFilesProcessor to delete the large file
    deleteFile(tableLoader(), largeFile.toString());

    // Verify that the large file has been deleted
    files = listFiles(table);
    assertThat(files).doesNotContain(largeFileName);
  }

  private void deleteFile(TableLoader tableLoader, String fileName) throws Exception {
    tableLoader.open();
    DeleteFilesProcessor deleteFilesProcessor =
        new DeleteFilesProcessor(table, DUMMY_TASK_NAME, 0, 10);
    try (OneInputStreamOperatorTestHarness<String, Void> testHarness =
        new OneInputStreamOperatorTestHarness<>(deleteFilesProcessor, StringSerializer.INSTANCE)) {
      testHarness.open();
      testHarness.processElement(fileName, System.currentTimeMillis());
      testHarness.processWatermark(EVENT_TIME);
      testHarness.endInput();
    }
  }

  @Test
  void testBatchDelete() throws Exception {
    // Simulate adding multiple files
    Set<String> filesToDelete = Sets.newHashSet(TABLE_FILES);
    filesToDelete.add("file1.txt");
    filesToDelete.add("file2.txt");

    // Use a smaller batch size to trigger batch deletion logic
    DeleteFilesProcessor deleteFilesProcessor =
        new DeleteFilesProcessor(table, DUMMY_TASK_NAME, 0, 2);
    try (OneInputStreamOperatorTestHarness<String, Void> testHarness =
        new OneInputStreamOperatorTestHarness<>(deleteFilesProcessor, StringSerializer.INSTANCE)) {
      testHarness.open();

      for (String file : filesToDelete) {
        testHarness.processElement(file, System.currentTimeMillis());
      }

      testHarness.processWatermark(EVENT_TIME);
      testHarness.endInput();

      // Verify that files are deleted
      assertThat(listFiles(table)).isEqualTo(TABLE_FILES);
      Counter succeededCounter = deleteFilesProcessor.getSucceededCounter();
      Counter failedCounter = deleteFilesProcessor.getFailedCounter();
      Histogram histogram = deleteFilesProcessor.getDeleteFileTimeMsHistogram();
      assertThat(succeededCounter.getCount()).isEqualTo(filesToDelete.size());
      assertThat(failedCounter.getCount()).isEqualTo(0);
      assertThat(histogram.getStatistics().getStdDev()).isGreaterThan(0); // Ensure there is data
    } finally {
      deleteFilesProcessor.close();
    }
  }

  @Test
  void testConcurrentDelete() throws Exception {
    Set<String> filesToDelete = Sets.newHashSet(TABLE_FILES);
    filesToDelete.add("file1.txt");
    filesToDelete.add("file2.txt");

    try (OneInputStreamOperatorTestHarness<String, Void> testHarness =
        new OneInputStreamOperatorTestHarness<>(
            new DeleteFilesProcessor(table, DUMMY_TASK_NAME, 0, 2), StringSerializer.INSTANCE)) {
      testHarness.open();

      // Simulate concurrent deletion of files
      ExecutorService executorService = Executors.newFixedThreadPool(2);
      for (String file : filesToDelete) {
        executorService.submit(
            () -> {
              try {
                testHarness.processElement(file, System.currentTimeMillis());
              } catch (Exception e) {
                e.printStackTrace();
              }
            });
      }

      executorService.shutdown();
      while (!executorService.isTerminated()) {}

      testHarness.processWatermark(EVENT_TIME);
      testHarness.endInput();

      // Verify that all files are deleted
      assertThat(listFiles(table)).isEqualTo(TABLE_FILES);
    }
  }

  @Test
  void testDeleteWithFailure() throws Exception {
    // Simulate adding files with some that will fail
    Set<String> filesToDelete = Sets.newHashSet(TABLE_FILES);
    filesToDelete.add("wrong://");

    DeleteFilesProcessor deleteFilesProcessor =
        new DeleteFilesProcessor(table, DUMMY_TASK_NAME, 0, 10);
    try (OneInputStreamOperatorTestHarness<String, Void> testHarness =
        new OneInputStreamOperatorTestHarness<>(deleteFilesProcessor, StringSerializer.INSTANCE)) {
      testHarness.open();

      for (String file : filesToDelete) {
        testHarness.processElement(file, System.currentTimeMillis());
      }

      testHarness.processWatermark(EVENT_TIME);
      testHarness.endInput();

      // Verify that failure count is updated and succeeded files are counted
      assertThat(deleteFilesProcessor.getSucceededCounter().getCount())
          .isEqualTo(TABLE_FILES.size());
      assertThat(deleteFilesProcessor.getFailedCounter().getCount()).isEqualTo(1);
    }
  }

  private static Path tablePath(Table table) {
    return FileSystems.getDefault().getPath(table.location().substring(5));
  }

  private static Set<String> listFiles(Table table) throws IOException {
    String tableRootPath = TestFixtures.TABLE_IDENTIFIER.toString().replace(".", "/");
    return Files.find(
            tablePath(table), Integer.MAX_VALUE, (filePath, fileAttr) -> fileAttr.isRegularFile())
        .map(
            p ->
                p.toString()
                    .substring(p.toString().indexOf(tableRootPath) + tableRootPath.length() + 1))
        .collect(Collectors.toSet());
  }
}

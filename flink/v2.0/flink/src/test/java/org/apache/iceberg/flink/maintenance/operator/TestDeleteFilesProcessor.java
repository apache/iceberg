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
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
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

    deleteFile(tableLoader(), dummyFile.toString(), true /* expectSuccess */);

    assertThat(listFiles(table)).isEqualTo(TABLE_FILES);
  }

  @Test
  void testDeleteMissingFile() throws Exception {
    Path dummyFile =
        FileSystems.getDefault().getPath(table.location().substring(5), DUMMY_FILE_NAME);

    deleteFile(tableLoader(), dummyFile.toString(), true /* expectSuccess */);

    assertThat(listFiles(table)).isEqualTo(TABLE_FILES);
  }

  @Test
  void testInvalidURIScheme() throws Exception {
    deleteFile(tableLoader(), "wrong://", false /* expectFail */);

    assertThat(listFiles(table)).isEqualTo(TABLE_FILES);
  }

  @Test
  void testDeleteNonExistentFile() throws Exception {
    String nonexistentFile = "nonexistentFile.txt";

    deleteFile(tableLoader(), nonexistentFile, true /* expectSuccess */);

    assertThat(listFiles(table)).isEqualTo(TABLE_FILES);
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
      assertThat(deleteFilesProcessor.succeededCounter().getCount())
          .isEqualTo(filesToDelete.size());
      assertThat(deleteFilesProcessor.failedCounter().getCount()).isEqualTo(0);
      assertThat(deleteFilesProcessor.maxDeleteTimeMs()).isGreaterThan(0);
    } finally {
      deleteFilesProcessor.close();
    }
  }

  @Test
  void testConcurrentDelete() throws Exception {
    Path root = tablePath(table);

    // Generate 30 test files: delete-0.txt to delete-29.txt
    Set<String> targets = Sets.newHashSet();

    for (int i = 0; i < 30; i++) {
      String fileName = "delete-" + i + ".txt";
      targets.add(fileName);
      Files.write(root.resolve(fileName), fileName.getBytes(StandardCharsets.UTF_8));
    }

    assertThat(listFiles(table)).containsAll(targets);

    DeleteFilesProcessor p1 = new DeleteFilesProcessor(table, DUMMY_TASK_NAME + "-p1", 0, 2);
    DeleteFilesProcessor p2 = new DeleteFilesProcessor(table, DUMMY_TASK_NAME + "-p2", 0, 2);

    // The two processors will attempt to delete the same files sequentially
    try (OneInputStreamOperatorTestHarness<String, Void> h1 =
            new OneInputStreamOperatorTestHarness<>(p1, StringSerializer.INSTANCE);
        OneInputStreamOperatorTestHarness<String, Void> h2 =
            new OneInputStreamOperatorTestHarness<>(p2, StringSerializer.INSTANCE)) {

      h1.open();
      h2.open();

      long ts = System.currentTimeMillis();

      // Process each file sequentially
      for (String f : targets) {
        // Process with p1 first
        h1.processElement(f, ts);
        // Then process with p2
        h2.processElement(f, ts);
      }

      h1.processWatermark(EVENT_TIME);
      h2.processWatermark(EVENT_TIME);
      h1.endInput();
      h2.endInput();

      // Verify metrics: each file should be attempted by both processors
      long success = p1.succeededCounter().getCount() + p2.succeededCounter().getCount();
      long fail = p1.failedCounter().getCount() + p2.failedCounter().getCount();

      assertThat(success + fail).isEqualTo(targets.size() * 2);
      assertThat(fail).isEqualTo(0);
    } finally {
      p1.close();
      p2.close();
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
      assertThat(deleteFilesProcessor.succeededCounter().getCount()).isEqualTo(TABLE_FILES.size());
      assertThat(deleteFilesProcessor.failedCounter().getCount()).isEqualTo(1);
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

  /**
   * Helper method to test file deletion using {@link DeleteFilesProcessor}.
   *
   * <p>This method simulates the deletion of a file from the table directory and validates the
   * metrics reported by {@link DeleteFilesProcessor}. It checks whether the deletion should succeed
   * or fail, based on the {@code expectSuccess} flag.
   *
   * @param tableLoader the table loader used to initialize the processor
   * @param fileName the name of the file to be deleted
   * @param expectSuccess true if the deletion is expected to succeed, false if it is expected to
   *     fail
   * @throws Exception if any error occurs during deletion or assertion
   */
  private void deleteFile(TableLoader tableLoader, String fileName, boolean expectSuccess)
      throws Exception {
    tableLoader.open();
    DeleteFilesProcessor deleteFilesProcessor =
        new DeleteFilesProcessor(table, DUMMY_TASK_NAME, 0, 10);
    try (OneInputStreamOperatorTestHarness<String, Void> testHarness =
        new OneInputStreamOperatorTestHarness<>(deleteFilesProcessor, StringSerializer.INSTANCE)) {
      testHarness.open();
      testHarness.processElement(fileName, System.currentTimeMillis());
      testHarness.processWatermark(EVENT_TIME);
      testHarness.endInput();

      // Validate if the metrics meet expectations
      if (expectSuccess) {
        assertThat(deleteFilesProcessor.succeededCounter().getCount()).isEqualTo(1);
        assertThat(deleteFilesProcessor.failedCounter().getCount()).isEqualTo(0);
        assertThat(deleteFilesProcessor.maxDeleteTimeMs()).isGreaterThan(0);
      } else {
        assertThat(deleteFilesProcessor.succeededCounter().getCount()).isEqualTo(0);
        assertThat(deleteFilesProcessor.failedCounter().getCount()).isEqualTo(1);
        assertThat(deleteFilesProcessor.maxDeleteTimeMs()).isGreaterThan(0);
      }

    } finally {
      deleteFilesProcessor.close();
    }
  }
}

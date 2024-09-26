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

  private void deleteFile(TableLoader tableLoader, String fileName) throws Exception {
    tableLoader().open();
    try (OneInputStreamOperatorTestHarness<String, Void> testHarness =
        new OneInputStreamOperatorTestHarness<>(
            new DeleteFilesProcessor(DUMMY_NAME, tableLoader.loadTable(), 10),
            StringSerializer.INSTANCE)) {
      testHarness.open();
      testHarness.processElement(fileName, System.currentTimeMillis());
      testHarness.processWatermark(EVENT_TIME);
      testHarness.endInput();
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

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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMicroBatchBuilder extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @BeforeEach
  public void setupTableProperties() {
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "3").commit();
  }

  @TestTemplate
  public void testGenerateMicroBatch() {
    add(table.newAppend(), files("A", "B", "C", "D", "E"));

    MicroBatch batch =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(0, 6, Long.MAX_VALUE, true);
    assertThat(batch.snapshotId()).isEqualTo(1L);
    assertThat(batch.startFileIndex()).isEqualTo(0);
    assertThat(batch.endFileIndex()).isEqualTo(5);
    assertThat(batch.sizeInBytes()).isEqualTo(50);
    assertThat(batch.lastIndexOfSnapshot()).isTrue();
    filesMatch(Lists.newArrayList("A", "B", "C", "D", "E"), filesToScan(batch.tasks()));

    MicroBatch batch1 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(0, 1, 15L, true);
    assertThat(batch1.endFileIndex()).isEqualTo(1);
    assertThat(batch1.sizeInBytes()).isEqualTo(10);
    assertThat(batch1.lastIndexOfSnapshot()).isFalse();
    filesMatch(Lists.newArrayList("A"), filesToScan(batch1.tasks()));

    MicroBatch batch2 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(batch1.endFileIndex(), 4, 30L, true);
    assertThat(batch2.endFileIndex()).isEqualTo(4);
    assertThat(batch2.sizeInBytes()).isEqualTo(30);
    assertThat(batch2.lastIndexOfSnapshot()).isFalse();
    filesMatch(Lists.newArrayList("B", "C", "D"), filesToScan(batch2.tasks()));

    MicroBatch batch3 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(batch2.endFileIndex(), 5, 50L, true);
    assertThat(batch3.endFileIndex()).isEqualTo(5);
    assertThat(batch3.sizeInBytes()).isEqualTo(10);
    assertThat(batch3.lastIndexOfSnapshot()).isTrue();
    filesMatch(Lists.newArrayList("E"), filesToScan(batch3.tasks()));
  }

  @TestTemplate
  public void testGenerateMicroBatchWithSmallTargetSize() {
    add(table.newAppend(), files("A", "B", "C", "D", "E"));

    MicroBatch batch =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(0, 1, 10L, true);
    assertThat(batch.snapshotId()).isEqualTo(1L);
    assertThat(batch.startFileIndex()).isEqualTo(0);
    assertThat(batch.endFileIndex()).isEqualTo(1);
    assertThat(batch.sizeInBytes()).isEqualTo(10);
    assertThat(batch.lastIndexOfSnapshot()).isFalse();
    filesMatch(Lists.newArrayList("A"), filesToScan(batch.tasks()));

    MicroBatch batch1 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(batch.endFileIndex(), 2, 5L, true);
    assertThat(batch1.endFileIndex()).isEqualTo(2);
    assertThat(batch1.sizeInBytes()).isEqualTo(10);
    filesMatch(Lists.newArrayList("B"), filesToScan(batch1.tasks()));
    assertThat(batch1.lastIndexOfSnapshot()).isFalse();

    MicroBatch batch2 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(batch1.endFileIndex(), 3, 10L, true);
    assertThat(batch2.endFileIndex()).isEqualTo(3);
    assertThat(batch2.sizeInBytes()).isEqualTo(10);
    filesMatch(Lists.newArrayList("C"), filesToScan(batch2.tasks()));
    assertThat(batch2.lastIndexOfSnapshot()).isFalse();

    MicroBatch batch3 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(batch2.endFileIndex(), 4, 10L, true);
    assertThat(batch3.endFileIndex()).isEqualTo(4);
    assertThat(batch3.sizeInBytes()).isEqualTo(10);
    filesMatch(Lists.newArrayList("D"), filesToScan(batch3.tasks()));
    assertThat(batch3.lastIndexOfSnapshot()).isFalse();

    MicroBatch batch4 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(batch3.endFileIndex(), 5, 5L, true);
    assertThat(batch4.endFileIndex()).isEqualTo(5);
    assertThat(batch4.sizeInBytes()).isEqualTo(10);
    filesMatch(Lists.newArrayList("E"), filesToScan(batch4.tasks()));
    assertThat(batch4.lastIndexOfSnapshot()).isTrue();

    MicroBatch batch5 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(batch4.endFileIndex(), 5, 5L, true);
    assertThat(batch5.endFileIndex()).isEqualTo(5);
    assertThat(batch5.sizeInBytes()).isEqualTo(0);
    assertThat(batch5.tasks()).isEmpty();
    assertThat(batch5.lastIndexOfSnapshot()).isTrue();
  }

  private static DataFile file(String name) {
    return DataFiles.builder(SPEC)
        .withPath(name + ".parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(1)
        .build();
  }

  private static void add(AppendFiles appendFiles, List<DataFile> adds) {
    for (DataFile f : adds) {
      appendFiles.appendFile(f);
    }
    appendFiles.commit();
  }

  private static void delete(DeleteFiles deleteFiles, List<DataFile> deletes) {
    for (DataFile f : deletes) {
      deleteFiles.deleteFile(f);
    }
    deleteFiles.commit();
  }

  private static List<DataFile> files(String... names) {
    return Lists.transform(Lists.newArrayList(names), TestMicroBatchBuilder::file);
  }

  private static List<String> filesToScan(Iterable<FileScanTask> tasks) {
    Iterable<String> filesToRead =
        Iterables.transform(
            tasks,
            t -> {
              String path = t.file().path().toString();
              return path.split("\\.")[0];
            });
    return Lists.newArrayList(filesToRead);
  }

  private static void filesMatch(List<String> expected, List<String> actual) {
    Collections.sort(expected);
    Collections.sort(actual);
    assertThat(actual).isEqualTo(expected);
  }
}

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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSnapshotSelection extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @TestTemplate
  public void testSnapshotSelectionById() {
    assertThat(listManifestFiles()).hasSize(0);

    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot firstSnapshot = table.currentSnapshot();

    table.newFastAppend().appendFile(FILE_B).commit();
    Snapshot secondSnapshot = table.currentSnapshot();

    assertThat(table.snapshots()).hasSize(2);
    validateSnapshot(null, table.snapshot(firstSnapshot.snapshotId()), FILE_A);
    validateSnapshot(firstSnapshot, table.snapshot(secondSnapshot.snapshotId()), FILE_B);
  }

  @TestTemplate
  public void testSnapshotStatsForAddedFiles() {
    DataFile fileWithStats =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-with-stats.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(10)
            .withMetrics(
                new Metrics(
                    3L,
                    null, // no column sizes
                    ImmutableMap.of(1, 3L), // value count
                    ImmutableMap.of(1, 0L), // null count
                    null,
                    ImmutableMap.of(1, longToBuffer(20L)), // lower bounds
                    ImmutableMap.of(1, longToBuffer(22L)))) // upper bounds
            .build();

    table.newFastAppend().appendFile(fileWithStats).commit();

    Snapshot snapshot = table.currentSnapshot();
    Iterable<DataFile> addedFiles = snapshot.addedDataFiles(table.io());
    assertThat(addedFiles).hasSize(1);
    DataFile dataFile = Iterables.getOnlyElement(addedFiles);
    assertThat(dataFile.valueCounts()).isNotNull();
    assertThat(dataFile.nullValueCounts()).isNotNull();
    assertThat(dataFile.lowerBounds()).isNotNull();
    assertThat(dataFile.upperBounds()).isNotNull();
  }

  private ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }
}

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
import java.util.List;
import java.util.Set;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestFindFiles extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  public static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @TestTemplate
  public void testBasicBehavior() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Iterable<DataFile> files = FindFiles.in(table).collect();

    assertThat(pathSet(files)).isEqualTo(pathSet(FILE_A, FILE_B));
  }

  @TestTemplate
  public void testWithMetadataMatching() {
    table
        .newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();

    Iterable<DataFile> files =
        FindFiles.in(table)
            .withMetadataMatching(Expressions.startsWith("file_path", "/path/to/data-a"))
            .collect();

    assertThat(pathSet(files)).isEqualTo(pathSet(FILE_A));
  }

  @TestTemplate
  public void testWithRecordsMatching() {
    table
        .newAppend()
        .appendFile(
            DataFiles.builder(SPEC)
                .withInputFile(Files.localInput("/path/to/data-e.parquet"))
                .withPartitionPath("data_bucket=4")
                .withMetrics(
                    new Metrics(
                        3L,
                        null, // no column sizes
                        ImmutableMap.of(1, 3L), // value count
                        ImmutableMap.of(1, 0L), // null count
                        null,
                        ImmutableMap.of(
                            1,
                            Conversions.toByteBuffer(Types.IntegerType.get(), 1)), // lower bounds
                        ImmutableMap.of(
                            1,
                            Conversions.toByteBuffer(Types.IntegerType.get(), 5)))) // lower bounds
                .build())
        .commit();

    final Iterable<DataFile> files =
        FindFiles.in(table).withRecordsMatching(Expressions.equal("id", 1)).collect();

    assertThat(pathSet(files)).containsExactly("/path/to/data-e.parquet");
  }

  @TestTemplate
  public void testInPartition() {
    table
        .newAppend()
        .appendFile(FILE_A) // bucket 0
        .appendFile(FILE_B) // bucket 1
        .appendFile(FILE_C) // bucket 2
        .appendFile(FILE_D) // bucket 3
        .commit();

    Iterable<DataFile> files =
        FindFiles.in(table)
            .inPartition(table.spec(), StaticDataTask.Row.of(1))
            .inPartition(table.spec(), StaticDataTask.Row.of(2))
            .collect();

    assertThat(pathSet(files)).isEqualTo(pathSet(FILE_B, FILE_C));
  }

  @TestTemplate
  public void testInPartitions() {
    table
        .newAppend()
        .appendFile(FILE_A) // bucket 0
        .appendFile(FILE_B) // bucket 1
        .appendFile(FILE_C) // bucket 2
        .appendFile(FILE_D) // bucket 3
        .commit();

    Iterable<DataFile> files =
        FindFiles.in(table)
            .inPartitions(table.spec(), StaticDataTask.Row.of(1), StaticDataTask.Row.of(2))
            .collect();

    assertThat(pathSet(files)).isEqualTo(pathSet(FILE_B, FILE_C));
  }

  @TestTemplate
  public void testAsOfTimestamp() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).commit();

    long timestamp = System.currentTimeMillis();

    table.newAppend().appendFile(FILE_C).commit();

    table.newAppend().appendFile(FILE_D).commit();

    Iterable<DataFile> files = FindFiles.in(table).asOfTime(timestamp).collect();

    assertThat(pathSet(files)).isEqualTo(pathSet(FILE_A, FILE_B));
  }

  @TestTemplate
  public void testSnapshotId() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.newAppend().appendFile(FILE_C).commit();

    long snapshotId = table.currentSnapshot().snapshotId();

    table.newAppend().appendFile(FILE_D).commit();

    Iterable<DataFile> files = FindFiles.in(table).inSnapshot(snapshotId).collect();

    assertThat(pathSet(files)).isEqualTo(pathSet(FILE_A, FILE_B, FILE_C));
  }

  @TestTemplate
  public void testCaseSensitivity() {
    table
        .newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();

    Iterable<DataFile> files =
        FindFiles.in(table)
            .caseInsensitive()
            .withMetadataMatching(Expressions.startsWith("FILE_PATH", "/path/to/data-a"))
            .collect();

    assertThat(pathSet(files)).isEqualTo(pathSet(FILE_A));
  }

  @TestTemplate
  public void testIncludeColumnStats() {
    table.newAppend().appendFile(FILE_WITH_STATS).commit();

    Iterable<DataFile> files = FindFiles.in(table).includeColumnStats().collect();
    final DataFile file = files.iterator().next();

    assertThat(file.columnSizes()).isEqualTo(FILE_WITH_STATS.columnSizes());
    assertThat(file.valueCounts()).isEqualTo(FILE_WITH_STATS.valueCounts());
    assertThat(file.nullValueCounts()).isEqualTo(FILE_WITH_STATS.nullValueCounts());
    assertThat(file.nanValueCounts()).isEqualTo(FILE_WITH_STATS.nanValueCounts());
    assertThat(file.lowerBounds()).isEqualTo(FILE_WITH_STATS.lowerBounds());
    assertThat(file.upperBounds()).isEqualTo(FILE_WITH_STATS.upperBounds());
  }

  @TestTemplate
  public void testNoSnapshot() {
    // a table has no snapshot when it just gets created and no data is loaded yet

    // if not handled properly, NPE will be thrown in collect()
    Iterable<DataFile> files = FindFiles.in(table).collect();

    // verify an empty collection of data file is returned
    assertThat(Sets.newHashSet(files)).hasSize(0);
  }

  private Set<String> pathSet(DataFile... files) {
    return Sets.newHashSet(
        Iterables.transform(Arrays.asList(files), file -> file.path().toString()));
  }

  private Set<String> pathSet(Iterable<DataFile> files) {
    return Sets.newHashSet(Iterables.transform(files, file -> file.path().toString()));
  }
}

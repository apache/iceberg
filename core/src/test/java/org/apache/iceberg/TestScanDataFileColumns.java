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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestScanDataFileColumns {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  private static final Configuration CONF = new Configuration();
  private static final Tables TABLES = new HadoopTables(CONF);

  @TempDir private Path temp;

  private String tableLocation = null;
  private Table table = null;

  @BeforeEach
  public void createTables() throws IOException {
    File location = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(location.delete()).isTrue();
    this.tableLocation = location.toString();
    this.table =
        TABLES.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name()),
            tableLocation);

    // commit the test data
    table
        .newAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("file1.parquet")
                .withFileSizeInBytes(100)
                .withMetrics(
                    new Metrics(
                        3L,
                        ImmutableMap.of(1, 50L, 2, 100L), // column size
                        ImmutableMap.of(1, 3L, 2, 5L), // value count
                        ImmutableMap.of(1, 0L, 2, 0L), // null count
                        null,
                        ImmutableMap.of(1, longToBuffer(0L), 2, longToBuffer(3L)), // lower bounds
                        ImmutableMap.of(1, longToBuffer(2L), 2, longToBuffer(4L)))) // upper bounds
                .build())
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("file2.parquet")
                .withFileSizeInBytes(100)
                .withMetrics(
                    new Metrics(
                        3L,
                        ImmutableMap.of(1, 60L, 2, 120L), // column size
                        ImmutableMap.of(1, 3L, 2, 10L), // value count
                        ImmutableMap.of(1, 0L, 2, 0L), // null count
                        null,
                        ImmutableMap.of(1, longToBuffer(10L), 2, longToBuffer(13L)), // lower bounds
                        ImmutableMap.of(
                            1, longToBuffer(12L), 2, longToBuffer(14L)))) // upper bounds
                .build())
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("file3.parquet")
                .withFileSizeInBytes(100)
                .withMetrics(
                    new Metrics(
                        3L,
                        ImmutableMap.of(1, 70L, 2, 140L), // column size
                        ImmutableMap.of(1, 3L, 2, 15L), // value count
                        ImmutableMap.of(1, 0L, 2, 0L), // null count
                        null,
                        ImmutableMap.of(1, longToBuffer(20L), 2, longToBuffer(23L)), // lower bounds
                        ImmutableMap.of(
                            1, longToBuffer(22L), 2, longToBuffer(24L)))) // upper bounds
                .build())
        .commit();
  }

  @Test
  public void testColumnStatsIgnored() {
    // stats columns should be suppressed by default
    for (FileScanTask fileTask : table.newScan().planFiles()) {
      assertThat(fileTask.file().valueCounts()).isNull();
      assertThat(fileTask.file().nullValueCounts()).isNull();
      assertThat(fileTask.file().lowerBounds()).isNull();
      assertThat(fileTask.file().upperBounds()).isNull();
    }
  }

  @Test
  public void testColumnStatsLoading() {
    // stats columns should be suppressed by default
    for (FileScanTask fileTask : table.newScan().includeColumnStats().planFiles()) {
      assertThat(fileTask.file().valueCounts()).hasSize(2);
      assertThat(fileTask.file().nullValueCounts()).hasSize(2);
      assertThat(fileTask.file().lowerBounds()).hasSize(2);
      assertThat(fileTask.file().upperBounds()).hasSize(2);
      assertThat(fileTask.file().columnSizes()).hasSize(2);
    }
  }

  @Test
  public void testColumnStatsPartial() {
    // stats columns should be suppressed by default
    for (FileScanTask fileTask :
        table.newScan().includeColumnStats(ImmutableSet.of("id")).planFiles()) {
      assertThat(fileTask.file().valueCounts()).hasSize(1);
      assertThat(fileTask.file().nullValueCounts()).hasSize(1);
      assertThat(fileTask.file().lowerBounds()).hasSize(1);
      assertThat(fileTask.file().upperBounds()).hasSize(1);
      assertThat(fileTask.file().columnSizes()).hasSize(1);
    }
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }
}

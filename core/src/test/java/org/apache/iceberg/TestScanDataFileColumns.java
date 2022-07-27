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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestScanDataFileColumns {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  private static final Configuration CONF = new Configuration();
  private static final Tables TABLES = new HadoopTables(CONF);

  @Rule public final TemporaryFolder temp = new TemporaryFolder();

  private String tableLocation = null;
  private Table table = null;

  @Before
  public void createTables() throws IOException {
    File location = temp.newFolder("shared");
    Assert.assertTrue(location.delete());
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
                        ImmutableMap.of(1, 50L), // column size
                        ImmutableMap.of(1, 3L), // value count
                        ImmutableMap.of(1, 0L), // null count
                        null,
                        ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
                        ImmutableMap.of(1, longToBuffer(2L)))) // upper bounds)
                .build())
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("file2.parquet")
                .withFileSizeInBytes(100)
                .withMetrics(
                    new Metrics(
                        3L,
                        ImmutableMap.of(1, 60L), // column size
                        ImmutableMap.of(1, 3L), // value count
                        ImmutableMap.of(1, 0L), // null count
                        null,
                        ImmutableMap.of(1, longToBuffer(10L)), // lower bounds
                        ImmutableMap.of(1, longToBuffer(12L)))) // upper bounds)
                .build())
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("file3.parquet")
                .withFileSizeInBytes(100)
                .withMetrics(
                    new Metrics(
                        3L,
                        ImmutableMap.of(1, 70L), // column size
                        ImmutableMap.of(1, 3L), // value count
                        ImmutableMap.of(1, 0L), // null count
                        null,
                        ImmutableMap.of(1, longToBuffer(20L)), // lower bounds
                        ImmutableMap.of(1, longToBuffer(22L)))) // upper bounds)
                .build())
        .commit();
  }

  @Test
  public void testColumnStatsIgnored() {
    // stats columns should be suppressed by default
    for (FileScanTask fileTask : table.newScan().planFiles()) {
      Assert.assertNull(fileTask.file().valueCounts());
      Assert.assertNull(fileTask.file().nullValueCounts());
      Assert.assertNull(fileTask.file().lowerBounds());
      Assert.assertNull(fileTask.file().upperBounds());
    }
  }

  @Test
  public void testColumnStatsLoading() {
    // stats columns should be suppressed by default
    for (FileScanTask fileTask : table.newScan().includeColumnStats().planFiles()) {
      Assert.assertNotNull(fileTask.file().valueCounts());
      Assert.assertNotNull(fileTask.file().nullValueCounts());
      Assert.assertNotNull(fileTask.file().lowerBounds());
      Assert.assertNotNull(fileTask.file().upperBounds());
      Assert.assertNotNull(fileTask.file().columnSizes());
    }
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }
}

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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestPartitionedDeltaWriter extends DeltaWriterTestBase {

  @Override
  @BeforeEach
  public void before() {
    super.before();
    // Override the schema for Partitioned CDC tests
    when(table.schema()).thenReturn(CDC_SCHEMA);
    when(table.spec()).thenReturn(SPEC);
  }

  @ParameterizedTest
  @CsvSource({"parquet,2", "parquet,3", "orc,2", "orc,3"})
  public void testPartitionedDeltaWriterDVMode(String format, int formatVersion) {
    mockTableFormatVersion(formatVersion);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    Record row1 = createCDCRecord(123L, "partition1", "C");
    Record row2 = createCDCRecord(234L, "partition2", "C");
    Record row3 = createCDCRecord(345L, "partition1", "C");

    WriteResult result =
        writeTest(ImmutableList.of(row1, row2, row3), config, PartitionedDeltaWriter.class);

    // With DV mode, no delete files should be created
    assertThat(result.dataFiles()).hasSize(2); // 2 partitions
    assertThat(result.dataFiles()).allMatch(file -> file.format() == FileFormat.fromString(format));
    assertThat(result.deleteFiles()).hasSize(0);
  }

  @ParameterizedTest
  @CsvSource({"parquet,2", "parquet,3", "orc,2", "orc,3"})
  public void testCDCOperationsAcrossPartitions(String format, int formatVersion) {
    mockTableFormatVersion(formatVersion);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    // Different operations in different partitions
    Record insert1 = createCDCRecord(1L, "partition-a", "C");
    Record insert2 = createCDCRecord(2L, "partition-b", "C");
    Record update1 = createCDCRecord(1L, "partition-a", "U");
    Record delete2 = createCDCRecord(2L, "partition-b", "D");

    WriteResult result =
        writeTest(
            ImmutableList.of(insert1, insert2, update1, delete2),
            config,
            PartitionedDeltaWriter.class);

    // 2 partitions with data files and delete files
    assertThat(result.dataFiles()).hasSize(2);
    assertThat(result.deleteFiles()).hasSize(2);
  }

  @ParameterizedTest
  @CsvSource({"parquet,2", "parquet,3", "orc,2", "orc,3"})
  public void testMultipleUpdatesPerPartition(String format, int formatVersion) {
    mockTableFormatVersion(formatVersion);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    // Multiple operations in the same partition
    Record insert1 = createCDCRecord(1L, "same-partition", "C");
    Record insert2 = createCDCRecord(2L, "same-partition", "C");
    Record update1 = createCDCRecord(1L, "same-partition", "U");
    Record update2 = createCDCRecord(2L, "same-partition", "U");

    WriteResult result =
        writeTest(
            ImmutableList.of(insert1, insert2, update1, update2),
            config,
            PartitionedDeltaWriter.class);

    // Single partition with all operations
    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);
  }

  @ParameterizedTest
  @CsvSource({"parquet,2", "parquet,3", "orc,2", "orc,3"})
  public void testPartitionedInsertOnly(String format, int formatVersion) {
    mockTableFormatVersion(formatVersion);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    Record insert1 = createCDCRecord(1L, "p1", "C");
    Record insert2 = createCDCRecord(2L, "p2", "C");
    Record insert3 = createCDCRecord(3L, "p3", "C");
    Record insert4 = createCDCRecord(4L, "p1", "R");

    WriteResult result =
        writeTest(
            ImmutableList.of(insert1, insert2, insert3, insert4),
            config,
            PartitionedDeltaWriter.class);

    assertThat(result.dataFiles()).hasSize(3);
    assertThat(result.deleteFiles()).hasSize(0);

    Arrays.asList(result.dataFiles())
        .forEach(
            file -> {
              assertThat(file.format()).isEqualTo(FileFormat.fromString(format));
            });
  }

  @ParameterizedTest
  @CsvSource({"parquet,2", "parquet,3", "orc,2", "orc,3"})
  public void testPartitionedDeleteOnly(String format, int formatVersion) {
    mockTableFormatVersion(formatVersion);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    // Inserts followed by deletes across partitions
    Record insert1 = createCDCRecord(1L, "pa", "C");
    Record insert2 = createCDCRecord(2L, "pb", "C");
    Record delete1 = createCDCRecord(1L, "pa", "D");
    Record delete2 = createCDCRecord(2L, "pb", "D");

    WriteResult result =
        writeTest(
            ImmutableList.of(insert1, insert2, delete1, delete2),
            config,
            PartitionedDeltaWriter.class);

    // 2 partitions with data and delete files
    assertThat(result.dataFiles()).hasSize(2);
    assertThat(result.deleteFiles()).hasSize(2);
  }

  @ParameterizedTest
  @CsvSource({"parquet,2", "parquet,3", "orc,2", "orc,3"})
  public void testMixedOperationsSinglePartition(String format, int formatVersion) {
    mockTableFormatVersion(formatVersion);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    // Mix of INSERT, UPDATE, DELETE in single partition
    Record insert1 = createCDCRecord(1L, "partition-x", "C");
    Record insert2 = createCDCRecord(2L, "partition-x", "C");
    Record insert3 = createCDCRecord(3L, "partition-x", "C");
    Record update1 = createCDCRecord(1L, "partition-x", "U");
    Record delete2 = createCDCRecord(2L, "partition-x", "D");

    WriteResult result =
        writeTest(
            ImmutableList.of(insert1, insert2, insert3, update1, delete2),
            config,
            PartitionedDeltaWriter.class);

    // Single partition with all operations
    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);
  }

  @ParameterizedTest
  @CsvSource({"parquet,2", "parquet,3", "orc,2", "orc,3"})
  public void testPartitionedNonUpsertMode(String format, int formatVersion) {
    mockTableFormatVersion(formatVersion);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(false); // Non-upsert mode
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    Record insert1 = createCDCRecord(1L, "part-a", "C");
    Record insert2 = createCDCRecord(2L, "part-b", "C");
    Record update1 = createCDCRecord(1L, "part-a", "U");
    Record delete2 = createCDCRecord(2L, "part-b", "D");

    WriteResult result =
        writeTest(
            ImmutableList.of(insert1, insert2, update1, delete2),
            config,
            PartitionedDeltaWriter.class);

    // 2 partitions with data and delete files
    assertThat(result.dataFiles()).hasSize(2);
    assertThat(result.deleteFiles()).hasSize(2);
  }

  @ParameterizedTest
  @CsvSource({"parquet,2", "parquet,3", "orc,2", "orc,3"})
  public void testEmptyPartitions(String format, int formatVersion) {
    mockTableFormatVersion(formatVersion);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    // Empty write should produce no files
    WriteResult result = writeTest(ImmutableList.of(), config, PartitionedDeltaWriter.class);

    assertThat(result.dataFiles()).isEmpty();
    assertThat(result.deleteFiles()).isEmpty();
  }

  @ParameterizedTest
  @CsvSource({"parquet,2", "parquet,3", "orc,2", "orc,3"})
  public void testNestedCDCMixedOperationsMultiplePartitions(String format, int formatVersion) {
    mockTableFormatVersion(formatVersion);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_cdc.op");

    // Update table schema to use nested CDC schema
    when(table.schema()).thenReturn(CDC_SCHEMA_NESTED);
    when(table.spec()).thenReturn(SPEC);

    // Multiple partitions with mixed INSERT, UPDATE, DELETE operations using nested CDC field
    Record row1 = createCDCRecord(1L, "insert-partition-a", CDC_SCHEMA_NESTED, "_cdc.op", "C");
    Record row2 = createCDCRecord(2L, "insert-partition-b", CDC_SCHEMA_NESTED, "_cdc.op", "R");
    Record row1Update =
        createCDCRecord(1L, "updated-partition-a", CDC_SCHEMA_NESTED, "_cdc.op", "U");
    Record row3 = createCDCRecord(3L, "insert-partition-a", CDC_SCHEMA_NESTED, "_cdc.op", "C");
    Record row2Delete =
        createCDCRecord(2L, "insert-partition-b", CDC_SCHEMA_NESTED, "_cdc.op", "D");

    WriteResult result =
        writeTest(
            ImmutableList.of(row1, row2, row1Update, row3, row2Delete),
            config,
            PartitionedDeltaWriter.class);

    // Multiple partitions with mixed operations
    assertThat(result.dataFiles()).isNotEmpty();
    assertThat(result.deleteFiles()).isNotEmpty();
  }
}

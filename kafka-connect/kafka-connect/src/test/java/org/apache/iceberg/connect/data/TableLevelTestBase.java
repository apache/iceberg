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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.BeforeEach;

/**
 * Shared infrastructure for table-level integration tests.
 *
 * <p>Provides parameterized test configuration, writer creation, commit helpers, and read-back
 * utilities. Subclasses define the schema, record factories, and sink config.
 *
 * <p>Tests write to real filesystem-backed Iceberg tables, commit transactions, and read back
 * actual row data — verifying end-to-end correctness rather than just WriteResult metadata.
 */
abstract class TableLevelTestBase extends TestBase {

  @Parameter(index = 1)
  protected FileFormat format;

  @Parameter(index = 2)
  private boolean partitioned;

  @Parameters(name = "formatVersion = {0}, format = {1}, partitioned = {2}")
  protected static List<Object[]> parameters() {
    List<Object[]> params = Lists.newArrayList();
    for (int version : new int[] {2, 3}) {
      for (FileFormat fmt : new FileFormat[] {FileFormat.PARQUET, FileFormat.ORC}) {
        for (boolean part : new boolean[] {false, true}) {
          params.add(new Object[] {version, fmt, part});
        }
      }
    }
    return params;
  }

  @Override
  @BeforeEach
  public void setupTable() throws Exception {
    this.metadataDir = new File(tableDir, "metadata");
  }

  /** Subclasses provide the schema for the test table. */
  protected abstract Schema schema();

  /** Subclasses provide the sink config appropriate for their test scenario. */
  protected abstract IcebergSinkConfig sinkConfig();

  protected void createAndInitTable() {
    PartitionSpec spec =
        partitioned
            ? PartitionSpec.builderFor(schema()).identity("partition").build()
            : PartitionSpec.unpartitioned();
    this.table = create(schema(), spec);
    table
        .updateProperties()
        .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, String.valueOf(8 * 1024))
        .defaultFormat(format)
        .commit();
  }

  // ==============================================================================
  // Writer creation — exercises the actual Kafka Connect RecordUtils code path
  // ==============================================================================

  protected WriteResult writeBatch(List<Record> rows) throws IOException {
    TableReference ref =
        TableReference.of("test_catalog", TableIdentifier.of("test"), UUID.randomUUID());
    try (TaskWriter<Record> writer = RecordUtils.createTableWriter(table, ref, sinkConfig())) {
      for (Record row : rows) {
        writer.write(row);
      }
      return writer.complete();
    }
  }

  // ==============================================================================
  // Commit + read-back helpers (same pattern as Flink's TestDeltaTaskWriter)
  // ==============================================================================

  protected void commitTransaction(WriteResult result) {
    RowDelta rowDelta = table.newRowDelta();
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
    rowDelta
        .validateDeletedFiles()
        .validateDataFilesExist(Lists.newArrayList(result.referencedDataFiles()))
        .commit();
  }

  /**
   * Commits multiple WriteResults in a single RowDelta. This simulates the recovery scenario where
   * the Coordinator accumulates DataWritten events from multiple commit cycles (e.g., due to commit
   * failure + retry, or control topic replay after restart) and commits them all together. All
   * files receive the same sequence number.
   */
  protected void commitCombined(WriteResult... results) {
    RowDelta rowDelta = table.newRowDelta();
    List<CharSequence> allReferencedFiles = Lists.newArrayList();
    for (WriteResult result : results) {
      Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
      Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
      allReferencedFiles.addAll(Lists.newArrayList(result.referencedDataFiles()));
    }
    rowDelta.validateDeletedFiles().validateDataFilesExist(allReferencedFiles).commit();
  }

  protected StructLikeSet actualRowSet(String... columns) throws IOException {
    StructLikeSet set = StructLikeSet.create(table.schema().select(columns).asStruct());
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).select(columns).build()) {
      reader.forEach(set::add);
    }
    return set;
  }
}

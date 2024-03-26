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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class FilterFilesTestBase<
    ScanT extends Scan<ScanT, T, G>, T extends ScanTask, G extends ScanTaskGroup<T>> {

  @Parameter(index = 0)
  protected int formatVersion;

  protected abstract ScanT newScan(Table table);

  @TempDir protected Path temp;
  private final Schema schema =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));
  private File tableDir = null;

  @BeforeEach
  public void setupTableDir() throws IOException {
    this.tableDir = Files.createTempDirectory(temp, "junit").toFile();
  }

  @AfterEach
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @TestTemplate
  public void testFilterFilesUnpartitionedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = TestTables.create(tableDir, "test", schema, spec, formatVersion);
    testFilterFiles(table);
  }

  @TestTemplate
  public void testCaseInsensitiveFilterFilesUnpartitionedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = TestTables.create(tableDir, "test", schema, spec, formatVersion);
    testCaseInsensitiveFilterFiles(table);
  }

  @TestTemplate
  public void testFilterFilesPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    Table table = TestTables.create(tableDir, "test", schema, spec, formatVersion);
    testFilterFiles(table);
  }

  @TestTemplate
  public void testCaseInsensitiveFilterFilesPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    Table table = TestTables.create(tableDir, "test", schema, spec, formatVersion);
    testCaseInsensitiveFilterFiles(table);
  }

  private void testFilterFiles(Table table) {
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
    lowerBounds.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1));
    upperBounds.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 2));

    Metrics metrics =
        new Metrics(
            2L,
            Maps.newHashMap(),
            Maps.newHashMap(),
            Maps.newHashMap(),
            null,
            lowerBounds,
            upperBounds);

    DataFile file =
        DataFiles.builder(table.spec())
            .withPath("/path/to/file.parquet")
            .withFileSizeInBytes(0)
            .withMetrics(metrics)
            .build();

    table.newAppend().appendFile(file).commit();

    table.refresh();

    ScanT emptyScan = newScan(table).filter(Expressions.equal("id", 5));
    assertThat(emptyScan.planFiles()).isEmpty();

    ScanT nonEmptyScan = newScan(table).filter(Expressions.equal("id", 1));
    assertThat(nonEmptyScan.planFiles()).hasSize(1);
  }

  private void testCaseInsensitiveFilterFiles(Table table) {
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
    lowerBounds.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1));
    upperBounds.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 2));

    Metrics metrics =
        new Metrics(
            2L,
            Maps.newHashMap(),
            Maps.newHashMap(),
            Maps.newHashMap(),
            null,
            lowerBounds,
            upperBounds);

    DataFile file =
        DataFiles.builder(table.spec())
            .withPath("/path/to/file.parquet")
            .withFileSizeInBytes(0)
            .withMetrics(metrics)
            .build();

    table.newAppend().appendFile(file).commit();

    table.refresh();

    ScanT emptyScan = newScan(table).caseSensitive(false).filter(Expressions.equal("ID", 5));
    assertThat(emptyScan.planFiles()).hasSize(0);

    ScanT nonEmptyScan = newScan(table).caseSensitive(false).filter(Expressions.equal("ID", 1));
    assertThat(nonEmptyScan.planFiles()).hasSize(1);
  }
}

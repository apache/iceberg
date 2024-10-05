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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestManifestReaderStats extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  private static final Map<Integer, Long> VALUE_COUNT = ImmutableMap.of(3, 3L);
  private static final Map<Integer, Long> NULL_VALUE_COUNTS = ImmutableMap.of(3, 0L);
  private static final Map<Integer, Long> NAN_VALUE_COUNTS = ImmutableMap.of(3, 1L);
  private static final Map<Integer, ByteBuffer> LOWER_BOUNDS =
      ImmutableMap.of(3, Conversions.toByteBuffer(Types.IntegerType.get(), 2));
  private static final Map<Integer, ByteBuffer> UPPER_BOUNDS =
      ImmutableMap.of(3, Conversions.toByteBuffer(Types.IntegerType.get(), 4));

  private static final Metrics METRICS =
      new Metrics(
          3L, null, VALUE_COUNT, NULL_VALUE_COUNTS, NAN_VALUE_COUNTS, LOWER_BOUNDS, UPPER_BOUNDS);
  private static final String FILE_PATH = "/path/to/data-a.parquet";
  private static final DataFile FILE =
      DataFiles.builder(SPEC)
          .withPath(FILE_PATH)
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0") // easy way to set partition data for now
          .withRecordCount(3)
          .withMetrics(METRICS)
          .build();

  @TestTemplate
  public void testReadIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)) {
      CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
      ManifestEntry<DataFile> entry = entries.iterator().next();
      assertFullStats(entry.file());
    }
  }

  @TestTemplate
  public void testReadEntriesWithFilterIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, FILE_IO).filterRows(Expressions.equal("id", 3))) {
      CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
      ManifestEntry<DataFile> entry = entries.iterator().next();
      assertFullStats(entry.file());
    }
  }

  @TestTemplate
  public void testReadIteratorWithFilterIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, FILE_IO).filterRows(Expressions.equal("id", 3))) {
      DataFile entry = reader.iterator().next();
      assertFullStats(entry);
    }
  }

  @TestTemplate
  public void testReadEntriesWithFilterAndSelectIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, FILE_IO)
            .select(ImmutableList.of("file_path"))
            .filterRows(Expressions.equal("id", 3))) {
      CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
      ManifestEntry<DataFile> entry = entries.iterator().next();
      assertFullStats(entry.file());
    }
  }

  @TestTemplate
  public void testReadIteratorWithFilterAndSelectDropsStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, FILE_IO)
            .select(ImmutableList.of("file_path"))
            .filterRows(Expressions.equal("id", 3))) {
      DataFile entry = reader.iterator().next();
      assertStatsDropped(entry);
    }
  }

  @TestTemplate
  public void testReadIteratorWithFilterAndSelectRecordCountDropsStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, FILE_IO)
            .select(ImmutableList.of("file_path", "record_count"))
            .filterRows(Expressions.equal("id", 3))) {
      DataFile entry = reader.iterator().next();
      assertStatsDropped(entry);
    }
  }

  @TestTemplate
  public void testReadIteratorWithFilterAndSelectStatsIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, FILE_IO)
            .select(ImmutableList.of("file_path", "value_counts"))
            .filterRows(Expressions.equal("id", 3))) {
      DataFile entry = reader.iterator().next();
      assertFullStats(entry);

      // explicitly call copyWithoutStats and ensure record count will not be dropped
      assertStatsDropped(entry.copyWithoutStats());
    }
  }

  @TestTemplate
  public void testReadIteratorWithProjectStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, FILE_IO)
            .project(new Schema(ImmutableList.of(DataFile.FILE_PATH, DataFile.VALUE_COUNTS)))) {
      DataFile entry = reader.iterator().next();

      assertThat(entry.path()).isEqualTo(FILE_PATH);
      assertThat(entry.valueCounts()).isEqualTo(VALUE_COUNT);
      assertThat(entry.columnSizes()).isNull();
      assertThat(entry.nullValueCounts()).isNull();
      assertThat(entry.nanValueCounts()).isNull();
      assertThat(entry.lowerBounds()).isNull();
      assertThat(entry.upperBounds()).isNull();
      assertNullRecordCount(entry);
    }
  }

  @TestTemplate
  public void testReadEntriesWithSelectNotProjectStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, FILE_IO).select(ImmutableList.of("file_path"))) {
      CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
      ManifestEntry<DataFile> entry = entries.iterator().next();
      DataFile dataFile = entry.file();

      // selected field is populated
      assertThat(dataFile.path()).isEqualTo(FILE_PATH);

      // not selected fields are all null and not projected
      assertThat(dataFile.columnSizes()).isNull();
      assertThat(dataFile.valueCounts()).isNull();
      assertThat(dataFile.nullValueCounts()).isNull();
      assertThat(dataFile.nanValueCounts()).isNull();
      assertThat(dataFile.lowerBounds()).isNull();
      assertThat(dataFile.upperBounds()).isNull();
      assertNullRecordCount(dataFile);
    }
  }

  @TestTemplate
  public void testReadEntriesWithSelectCertainStatNotProjectStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, FILE_IO)
            .select(ImmutableList.of("file_path", "value_counts"))) {
      DataFile dataFile = reader.iterator().next();

      // selected fields are populated
      assertThat(dataFile.path()).isEqualTo(FILE_PATH);
      assertThat(dataFile.valueCounts()).isEqualTo(VALUE_COUNT);

      // not selected fields are all null and not projected
      assertThat(dataFile.columnSizes()).isNull();
      assertThat(dataFile.nullValueCounts()).isNull();
      assertThat(dataFile.nanValueCounts()).isNull();
      assertThat(dataFile.lowerBounds()).isNull();
      assertThat(dataFile.upperBounds()).isNull();
      assertNullRecordCount(dataFile);
    }
  }

  private void assertFullStats(DataFile dataFile) {
    assertThat(dataFile.recordCount()).isEqualTo(3);
    assertThat(dataFile.columnSizes()).isNull();
    assertThat(dataFile.valueCounts()).isEqualTo(VALUE_COUNT);
    assertThat(dataFile.nullValueCounts()).isEqualTo(NULL_VALUE_COUNTS);
    assertThat(dataFile.nanValueCounts()).isEqualTo(NAN_VALUE_COUNTS);
    assertThat(dataFile.lowerBounds()).isEqualTo(LOWER_BOUNDS);
    assertThat(dataFile.upperBounds()).isEqualTo(UPPER_BOUNDS);

    if (dataFile.valueCounts() != null) {
      assertThatThrownBy(() -> dataFile.valueCounts().clear(), "Should not be modifiable")
          .isInstanceOf(UnsupportedOperationException.class);
    }

    if (dataFile.nullValueCounts() != null) {
      assertThatThrownBy(() -> dataFile.nullValueCounts().clear(), "Should not be modifiable")
          .isInstanceOf(UnsupportedOperationException.class);
    }

    if (dataFile.nanValueCounts() != null) {
      assertThatThrownBy(() -> dataFile.nanValueCounts().clear(), "Should not be modifiable")
          .isInstanceOf(UnsupportedOperationException.class);
    }

    if (dataFile.upperBounds() != null) {
      assertThatThrownBy(() -> dataFile.upperBounds().clear(), "Should not be modifiable")
          .isInstanceOf(UnsupportedOperationException.class);
    }

    if (dataFile.lowerBounds() != null) {
      assertThatThrownBy(() -> dataFile.lowerBounds().clear(), "Should not be modifiable")
          .isInstanceOf(UnsupportedOperationException.class);
    }

    if (dataFile.columnSizes() != null) {
      assertThatThrownBy(() -> dataFile.columnSizes().clear(), "Should not be modifiable")
          .isInstanceOf(UnsupportedOperationException.class);
    }

    assertThat(dataFile.path()).isEqualTo(FILE_PATH); // always select file path in all test cases
  }

  private void assertStatsDropped(DataFile dataFile) {
    assertThat(dataFile.recordCount())
        .isEqualTo(3); // record count is not considered as droppable stats
    assertThat(dataFile.columnSizes()).isNull();
    assertThat(dataFile.valueCounts()).isNull();
    assertThat(dataFile.nullValueCounts()).isNull();
    assertThat(dataFile.nanValueCounts()).isNull();
    assertThat(dataFile.lowerBounds()).isNull();
    assertThat(dataFile.upperBounds()).isNull();

    assertThat(dataFile.path()).isEqualTo(FILE_PATH); // always select file path in all test cases
  }

  private void assertNullRecordCount(DataFile dataFile) {
    // record count is a primitive type, accessing null record count will throw NPE
    assertThatThrownBy(dataFile::recordCount).isInstanceOf(NullPointerException.class);
  }
}

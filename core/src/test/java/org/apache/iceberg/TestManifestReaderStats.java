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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.stats.BaseContentStats;
import org.apache.iceberg.stats.BaseFieldStats;
import org.apache.iceberg.stats.ContentStats;
import org.apache.iceberg.stats.FieldStats;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestManifestReaderStats extends TestBase {
  private static final Map<Integer, Long> VALUE_COUNT = ImmutableMap.of(1, 3L);
  private static final Map<Integer, Long> NULL_VALUE_COUNTS = ImmutableMap.of(1, 0L);
  private static final Map<Integer, Long> NAN_VALUE_COUNTS = ImmutableMap.of(1, 1L);
  private static final Map<Integer, ByteBuffer> LOWER_BOUNDS =
      ImmutableMap.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 2));
  private static final Map<Integer, ByteBuffer> UPPER_BOUNDS =
      ImmutableMap.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 4));

  private static final Metrics METRICS =
      new Metrics(
          3L, null, VALUE_COUNT, NULL_VALUE_COUNTS, NAN_VALUE_COUNTS, LOWER_BOUNDS, UPPER_BOUNDS);
  private static final String FILE_PATH = "/path/to/data-a.parquet";
  private static final FieldStats STAT =
      BaseFieldStats.builder()
          .type(Types.IntegerType.get())
          .fieldId(1)
          .valueCount(3L)
          .nullValueCount(0L)
          .nanValueCount(1L)
          .lowerBound(2)
          .upperBound(4)
          .build();
  private static final BaseContentStats STATS =
      BaseContentStats.builder().withFieldStats(STAT).build();

  private DataFile dataFile() {
    DataFiles.Builder builder =
        DataFiles.builder(SPEC)
            .withPath(FILE_PATH)
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0") // easy way to set partition data for now
            .withRecordCount(3);
    if (formatVersion <= 3) {
      return builder.withMetrics(METRICS).build();
    }

    return builder.withContentStats(STATS).build();
  }

  @TestTemplate
  public void testReadIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, dataFile());
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)) {
      CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
      ManifestEntry<DataFile> entry = entries.iterator().next();
      assertFullStats(entry.file());
    }
  }

  @TestTemplate
  public void testReadEntriesWithFilterIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, dataFile());
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, FILE_IO).filterRows(Expressions.equal("id", 3))) {
      CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
      ManifestEntry<DataFile> entry = entries.iterator().next();
      assertFullStats(entry.file());
    }
  }

  @TestTemplate
  public void testReadIteratorWithFilterIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, dataFile());
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, FILE_IO).filterRows(Expressions.equal("id", 3))) {
      DataFile entry = reader.iterator().next();
      assertFullStats(entry);
    }
  }

  @TestTemplate
  public void testReadEntriesWithFilterAndSelectIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, dataFile());
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
    ManifestFile manifest = writeManifest(1000L, dataFile());
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
    ManifestFile manifest = writeManifest(1000L, dataFile());
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
    ManifestFile manifest = writeManifest(1000L, dataFile());
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
    ManifestFile manifest = writeManifest(1000L, dataFile());

    if (formatVersion <= 3) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, FILE_IO)
              .project(new Schema(ImmutableList.of(DataFile.FILE_PATH, DataFile.VALUE_COUNTS)))) {
        DataFile entry = reader.iterator().next();

        assertThat(entry.location()).isEqualTo(FILE_PATH);
        assertThat(entry.valueCounts()).isEqualTo(VALUE_COUNT);
        assertThat(entry.columnSizes()).isNull();
        assertThat(entry.nullValueCounts()).isNull();
        assertThat(entry.nanValueCounts()).isNull();
        assertThat(entry.lowerBounds()).isNull();
        assertThat(entry.upperBounds()).isNull();
        assertThat(entry.recordCount()).isEqualTo(dataFile().recordCount());
      }
    } else {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, FILE_IO)
              .project(
                  new Schema(
                      ImmutableList.of(
                          DataFile.FILE_PATH,
                          optional(
                              146,
                              "content_stats",
                              Types.StructType.of(
                                  optional(
                                      10200,
                                      "1",
                                      Types.StructType.of(
                                          optional(10202, "value_count", Types.LongType.get())))),
                              "Column statistics"))))) {
        DataFile entry = reader.iterator().next();

        assertThat(entry.location()).isEqualTo(FILE_PATH);
        assertThat(entry.contentStats()).isNotNull();
        FieldStats stat = entry.contentStats().statsFor(1);
        assertThat(stat).isNotNull();
        assertThat(stat.valueCount()).isEqualTo(STAT.valueCount());
        assertThat(stat.nullValueCount()).isNull();
        assertThat(stat.nanValueCount()).isNull();
        assertThat(stat.lowerBound()).isNull();
        assertThat(stat.upperBound()).isNull();
      }
    }
  }

  @TestTemplate
  public void testReadEntriesWithSelectNotProjectStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, dataFile());
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, FILE_IO).select(ImmutableList.of("file_path"))) {
      CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
      ManifestEntry<DataFile> entry = entries.iterator().next();
      DataFile dataFile = entry.file();

      // selected field is populated
      assertThat(dataFile.location()).isEqualTo(FILE_PATH);

      // not selected fields are all null and not projected
      assertThat(dataFile.columnSizes()).isNull();
      assertThat(dataFile.valueCounts()).isNull();
      assertThat(dataFile.nullValueCounts()).isNull();
      assertThat(dataFile.nanValueCounts()).isNull();
      assertThat(dataFile.lowerBounds()).isNull();
      assertThat(dataFile.upperBounds()).isNull();
      assertThat(dataFile.recordCount()).isEqualTo(dataFile().recordCount());
    }
  }

  @TestTemplate
  public void testReadEntriesWithSelectCertainStatNotProjectStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, dataFile());
    if (formatVersion <= 3) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, FILE_IO)
              .select(ImmutableList.of("file_path", "value_counts"))) {
        DataFile dataFile = reader.iterator().next();

        // selected fields are populated
        assertThat(dataFile.location()).isEqualTo(FILE_PATH);
        assertThat(dataFile.valueCounts()).isEqualTo(VALUE_COUNT);

        // not selected fields are all null and not projected
        assertThat(dataFile.columnSizes()).isNull();
        assertThat(dataFile.nullValueCounts()).isNull();
        assertThat(dataFile.nanValueCounts()).isNull();
        assertThat(dataFile.lowerBounds()).isNull();
        assertThat(dataFile.upperBounds()).isNull();
        assertThat(dataFile.recordCount()).isEqualTo(dataFile().recordCount());
      }
    } else {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, FILE_IO)
              .select(ImmutableList.of("file_path", "content_stats.1.value_count"))) {
        DataFile dataFile = reader.iterator().next();
        assertThat(dataFile.contentStats()).isNotNull();
        FieldStats stat = dataFile.contentStats().statsFor(1);
        assertThat(stat).isNotNull();

        // selected fields are populated
        assertThat(dataFile.location()).isEqualTo(FILE_PATH);
        assertThat(stat.valueCount()).isEqualTo(STAT.valueCount());

        // not selected fields are all null and not projected
        assertThat(stat.nullValueCount()).isNull();
        assertThat(stat.nanValueCount()).isNull();
        assertThat(stat.lowerBound()).isNull();
        assertThat(stat.upperBound()).isNull();
      }
    }
  }

  private void assertFullStats(DataFile dataFile) {
    assertThat(dataFile.location())
        .isEqualTo(FILE_PATH); // always select file path in all test cases
    assertThat(dataFile.recordCount()).isEqualTo(3);

    if (formatVersion <= 3) {
      assertThat(dataFile.columnSizes()).isNull();
      assertThat(dataFile.valueCounts()).isEqualTo(VALUE_COUNT);
      assertThat(dataFile.nullValueCounts()).isEqualTo(NULL_VALUE_COUNTS);
      assertThat(dataFile.nanValueCounts()).isEqualTo(NAN_VALUE_COUNTS);
      assertThat(dataFile.lowerBounds()).isEqualTo(LOWER_BOUNDS);
      assertThat(dataFile.upperBounds()).isEqualTo(UPPER_BOUNDS);

      if (dataFile.valueCounts() != null) {
        assertThatThrownBy(() -> dataFile.valueCounts().clear(), "Should not be modifiable")
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage(null);
      }

      if (dataFile.nullValueCounts() != null) {
        assertThatThrownBy(() -> dataFile.nullValueCounts().clear(), "Should not be modifiable")
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage(null);
      }

      if (dataFile.nanValueCounts() != null) {
        assertThatThrownBy(() -> dataFile.nanValueCounts().clear(), "Should not be modifiable")
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage(null);
      }

      if (dataFile.upperBounds() != null) {
        assertThatThrownBy(() -> dataFile.upperBounds().clear(), "Should not be modifiable")
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage(null);
      }

      if (dataFile.lowerBounds() != null) {
        assertThatThrownBy(() -> dataFile.lowerBounds().clear(), "Should not be modifiable")
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage(null);
      }

      if (dataFile.columnSizes() != null) {
        assertThatThrownBy(() -> dataFile.columnSizes().clear(), "Should not be modifiable")
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage(null);
      }
    } else {
      ContentStats stats = dataFile.contentStats();
      assertThat(stats).isNotNull();
      assertThat(stats.fieldStats()).contains(STAT);
    }
  }

  private void assertStatsDropped(DataFile dataFile) {
    assertThat(dataFile.location())
        .isEqualTo(FILE_PATH); // always select file path in all test cases
    assertThat(dataFile.recordCount())
        .isEqualTo(3); // record count is not considered as droppable stats

    if (formatVersion <= 3) {
      assertThat(dataFile.columnSizes()).isNull();
      assertThat(dataFile.valueCounts()).isNull();
      assertThat(dataFile.nullValueCounts()).isNull();
      assertThat(dataFile.nanValueCounts()).isNull();
      assertThat(dataFile.lowerBounds()).isNull();
      assertThat(dataFile.upperBounds()).isNull();
    } else {
      assertThat(dataFile.contentStats()).isNull();
    }
  }
}

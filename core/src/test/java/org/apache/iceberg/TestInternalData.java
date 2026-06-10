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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestInternalData {

  @Parameter(index = 0)
  private FileFormat format;

  @Parameters(name = " format = {0}")
  protected static List<FileFormat> parameters() {
    return Arrays.asList(FileFormat.AVRO, FileFormat.PARQUET);
  }

  private static final Schema SIMPLE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()));

  private static final Schema NESTED_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "outer_id", Types.LongType.get()),
          Types.NestedField.optional(
              2,
              "nested_struct",
              Types.StructType.of(
                  Types.NestedField.optional(3, "inner_id", Types.LongType.get()),
                  Types.NestedField.optional(4, "inner_name", Types.StringType.get()))));

  @TempDir private Path tempDir;

  private final FileIO fileIO = new TestTables.LocalFileIO();

  @TestTemplate
  public void testCustomRootType() throws IOException {
    OutputFile outputFile = fileIO.newOutputFile(tempDir.resolve("test." + format).toString());

    List<Record> testData = RandomInternalData.generate(SIMPLE_SCHEMA, 1000, 1L);

    try (FileAppender<Record> appender =
        InternalData.write(format, outputFile).schema(SIMPLE_SCHEMA).build()) {
      appender.addAll(testData);
    }

    InputFile inputFile = fileIO.newInputFile(outputFile.location());
    List<PartitionData> readRecords = Lists.newArrayList();

    try (CloseableIterable<PartitionData> reader =
        InternalData.read(format, inputFile)
            .project(SIMPLE_SCHEMA)
            .setRootType(PartitionData.class)
            .build()) {
      for (PartitionData record : reader) {
        readRecords.add(record);
      }
    }

    assertThat(readRecords).hasSameSizeAs(testData);

    for (int i = 0; i < testData.size(); i++) {
      Record expected = testData.get(i);
      PartitionData actual = readRecords.get(i);

      assertThat(actual.get(0, Long.class)).isEqualTo(expected.get(0, Long.class));
      assertThat(actual.get(1, String.class)).isEqualTo(expected.get(1, String.class));
    }
  }

  /** Avro ignores withFilterHint — all rows are returned regardless of the hint. */
  @TestTemplate
  public void testWithFilterHintIsNoOpForAvro() throws IOException {
    assumeThat(format).as("Avro-only: hint must be a no-op").isEqualTo(FileFormat.AVRO);

    OutputFile outputFile = fileIO.newOutputFile(tempDir.resolve("test." + format).toString());

    List<Record> records = buildRecords(1L, 2L, 3L, 4L, 5L);

    try (FileAppender<Record> appender =
        InternalData.write(format, outputFile).schema(SIMPLE_SCHEMA).build()) {
      appender.addAll(records);
    }

    // Apply a hint that would match nothing — Avro must still return all rows.
    List<Record> result = Lists.newArrayList();
    try (CloseableIterable<Record> reader =
        InternalData.read(
                format,
                fileIO.newInputFile(outputFile.location()),
                Expressions.greaterThan("id", 1000L))
            .project(SIMPLE_SCHEMA)
            .build()) {
      reader.forEach(result::add);
    }

    assertThat(result).hasSize(records.size());
  }

  /**
   * Verifies that withFilterHint actually triggers Parquet row-group skipping.
   *
   * <p>Two non-overlapping id ranges are written into separate row groups by using a very small
   * row-group size (1 byte) and a min-record-check count of 1, forcing a flush after every row. The
   * filter hint targets only the high range, so the low-range row groups must be skipped. Without a
   * residual filter the count proves whether skipping happened: if row-group skipping works the
   * reader returns only the high-range rows; if it were disabled it would return all rows.
   */
  @TestTemplate
  public void testWithFilterHintEnablesRowGroupSkippingForParquet() throws IOException {
    assumeThat(format).as("Parquet-only: row-group skipping").isEqualTo(FileFormat.PARQUET);

    OutputFile outputFile = fileIO.newOutputFile(tempDir.resolve("test." + format).toString());

    // Low group: ids 1-5 (max = 5)
    List<Record> lowRecords = buildRecords(1L, 2L, 3L, 4L, 5L);
    // High group: ids 1001-1005 (min = 1001)
    List<Record> highRecords = buildRecords(1001L, 1002L, 1003L, 1004L, 1005L);

    // Force each record into its own row group so the id ranges don't mix.
    try (FileAppender<Record> appender =
        InternalData.write(format, outputFile)
            .schema(SIMPLE_SCHEMA)
            // 1-byte threshold + check every record = flush after every row
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1")
            .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "1")
            .build()) {
      appender.addAll(lowRecords);
      appender.addAll(highRecords);
    }

    // Apply the hint with NO residual filter.
    // Parquet row-group stats: low groups have max=5 < 100 → skipped.
    //                          high groups have min=1001 > 100 → read.
    // If skipping works: only 5 high-range records are returned.
    // If skipping were broken: all 10 records would be returned.
    List<Record> result = Lists.newArrayList();
    try (CloseableIterable<Record> reader =
        InternalData.read(
                format,
                fileIO.newInputFile(outputFile.location()),
                Expressions.greaterThan("id", 100L))
            .project(SIMPLE_SCHEMA)
            .build()) {
      reader.forEach(result::add);
    }

    assertThat(result)
        .as("Only high-range row groups should be read; low-range groups must be skipped")
        .hasSize(highRecords.size());
    assertThat(result).allSatisfy(r -> assertThat(r.get(0, Long.class)).isGreaterThan(100L));
  }

  /**
   * A selective hint that matches no row group must skip them all. Using the same forced
   * one-row-group-per-record layout as {@link #testWithFilterHintEnablesRowGroupSkippingForParquet}
   * and NO residual filter, a hint of {@code id > 100000} (above every written value) must return
   * zero rows for Parquet. This only happens if the hint is wired in and drives row-group
   * elimination; if the wiring regressed, all 10 rows would be returned.
   */
  @TestTemplate
  public void testWithFilterHintSkipsAllRowGroupsWhenNoneMatchForParquet() throws IOException {
    assumeThat(format).as("Parquet-only: row-group skipping").isEqualTo(FileFormat.PARQUET);

    OutputFile outputFile = fileIO.newOutputFile(tempDir.resolve("test." + format).toString());

    List<Record> lowRecords = buildRecords(1L, 2L, 3L, 4L, 5L);
    List<Record> highRecords = buildRecords(1001L, 1002L, 1003L, 1004L, 1005L);

    try (FileAppender<Record> appender =
        InternalData.write(format, outputFile)
            .schema(SIMPLE_SCHEMA)
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1")
            .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "1")
            .build()) {
      appender.addAll(lowRecords);
      appender.addAll(highRecords);
    }

    // Hint matches nothing (every id <= 1005). All row groups must be eliminated, so with no
    // residual filter the reader returns zero rows. Without the hint, all 10 rows would be read.
    List<Record> result = Lists.newArrayList();
    try (CloseableIterable<Record> reader =
        InternalData.read(
                format,
                fileIO.newInputFile(outputFile.location()),
                Expressions.greaterThan("id", 100000L))
            .project(SIMPLE_SCHEMA)
            .build()) {
      reader.forEach(result::add);
    }

    assertThat(result).as("hint matches no row group, so all groups must be skipped").isEmpty();
  }

  /**
   * The correct two-phase usage: withFilterHint for I/O skipping + residual filter for exactness.
   * Both formats must return only matching rows.
   */
  @TestTemplate
  public void testWithFilterHintAndResidualFilterReturnsMatchingRowsOnly() throws IOException {
    OutputFile outputFile = fileIO.newOutputFile(tempDir.resolve("test." + format).toString());

    List<Record> lowRecords = buildRecords(1L, 2L, 3L, 4L, 5L);
    List<Record> highRecords = buildRecords(1001L, 1002L, 1003L, 1004L, 1005L);

    try (FileAppender<Record> appender =
        InternalData.write(format, outputFile).schema(SIMPLE_SCHEMA).build()) {
      appender.addAll(lowRecords);
      appender.addAll(highRecords);
    }

    // Phase 1: hint (row-group skip for Parquet, no-op for Avro).
    // Phase 2: residual filter for correctness on both formats.
    List<Record> result = Lists.newArrayList();
    try (CloseableIterable<Record> base =
            InternalData.read(
                    format,
                    fileIO.newInputFile(outputFile.location()),
                    Expressions.greaterThan("id", 100L))
                .project(SIMPLE_SCHEMA)
                .build();
        CloseableIterable<Record> filtered =
            CloseableIterable.filter(base, r -> (Long) r.get(0) > 100L)) {
      filtered.forEach(result::add);
    }

    assertThat(result).hasSize(highRecords.size());
    assertThat(result).allSatisfy(r -> assertThat(r.get(0, Long.class)).isGreaterThan(100L));
  }

  private List<Record> buildRecords(Long... ids) {
    List<Record> records = Lists.newArrayList();
    for (Long id : ids) {
      GenericRecord record = GenericRecord.create(SIMPLE_SCHEMA);
      record.set(0, id);
      record.set(1, "name-" + id);
      records.add(record);
    }
    return records;
  }

  @TestTemplate
  public void testCustomTypeForNestedField() throws IOException {
    OutputFile outputFile = fileIO.newOutputFile(tempDir.resolve("test." + format).toString());

    List<Record> testData = RandomInternalData.generate(NESTED_SCHEMA, 1000, 1L);

    try (FileAppender<Record> appender =
        InternalData.write(format, outputFile).schema(NESTED_SCHEMA).build()) {
      appender.addAll(testData);
    }

    InputFile inputFile = fileIO.newInputFile(outputFile.location());
    List<Record> readRecords = Lists.newArrayList();

    try (CloseableIterable<Record> reader =
        InternalData.read(format, inputFile)
            .project(NESTED_SCHEMA)
            .setCustomType(2, TestHelpers.CustomRow.class)
            .build()) {
      for (Record record : reader) {
        readRecords.add(record);
      }
    }

    assertThat(readRecords).hasSameSizeAs(testData);

    for (int i = 0; i < testData.size(); i++) {
      Record expected = testData.get(i);
      Record actual = readRecords.get(i);

      assertThat(actual.get(0, Long.class)).isEqualTo(expected.get(0, Long.class));

      Object expectedNested = expected.get(1);
      Object actualNested = actual.get(1);

      if (expectedNested == null) {
        // Expected nested struct is null, so actual should also be null
        assertThat(actualNested).isNull();
      } else {
        // Expected nested struct is not null, so actual should be a CustomRow
        assertThat(actualNested).isNotNull();
        assertThat(actualNested)
            .as("Custom type should be TestHelpers.CustomRow but was: " + actualNested.getClass())
            .isInstanceOf(TestHelpers.CustomRow.class);
        TestHelpers.CustomRow customRow = (TestHelpers.CustomRow) actualNested;
        Record expectedRecord = (Record) expectedNested;

        assertThat(customRow.get(0, Long.class))
            .isEqualTo(expectedRecord.get(0, Long.class)); // inner_id
        assertThat(customRow.get(1, String.class))
            .isEqualTo(expectedRecord.get(1, String.class)); // inner_name
      }
    }
  }
}

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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Int96TestUtil.Row;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestGenericParquetInt96 {
  private static final BigInteger NANOS_PER_SECOND = BigInteger.valueOf(1_000_000_000L);
  @TempDir private Path temp;

  @ParameterizedTest
  @CsvSource({
    "false,false,false", "false,false,true", "false,true,false", "false,true,true",
    "true,false,false", "true,false,true", "true,true,false", "true,true,true"
  })
  void preservesExactValuesAndNullsAcrossRowGroups(boolean dictionary, boolean nanos, boolean zoned)
      throws IOException {
    BigInteger scale = BigInteger.valueOf(nanos ? 1 : 1_000);
    List<BigInteger> values =
        Arrays.asList(
            BigInteger.ZERO,
            BigInteger.valueOf(nanos ? -1 : -1_000),
            BigInteger.valueOf(nanos ? 1 : 1_000),
            BigInteger.valueOf(Long.MIN_VALUE).multiply(scale),
            BigInteger.valueOf(Long.MAX_VALUE).multiply(scale),
            BigInteger.valueOf(nanos ? 1_001 : 1_234_567_890_000L),
            null,
            BigInteger.valueOf(nanos ? -1_001 : -1_001_000));
    List<Row> expected = Lists.newArrayList();
    for (int index = 0; index < 32; index += 1) {
      expected.add(
          new Row(
              index,
              values.get(index % values.size()),
              index % 5 != 0,
              index % 7 == 1 ? null : values.get(index % values.size())));
    }

    File file = Int96TestUtil.writeRows(temp.resolve("exact.parquet"), dictionary, expected);
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file.toPath()))) {
      assertThat(reader.getFooter().getBlocks()).hasSizeGreaterThanOrEqualTo(2);
    }

    Schema schema = schema(nanos, zoned);
    for (boolean reuse : List.of(false, true)) {
      Parquet.ReadBuilder builder = Parquet.read(Files.localInput(file)).project(schema);
      if (reuse) {
        builder.reuseContainers();
      }

      try (CloseableIterable<Record> records =
          builder
              .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
              .build()) {
        int index = 0;
        for (Record actual : records) {
          Row row = expected.get(index);
          assertThat(actual.getField("id")).isEqualTo(row.id());
          assertThat(actual.getField("ts"))
              .isEqualTo(expectedTimestamp(row.timestampNanos(), nanos, zoned));
          if (row.parentPresent()) {
            Record nested = (Record) actual.getField("nested");
            assertThat(nested).isNotNull();
            Object child = expectedTimestamp(row.nestedTimestampNanos(), nanos, zoned);
            assertThat(nested.getField("ts")).isEqualTo(child);
            assertThat(actual.getField("items")).isEqualTo(Arrays.asList(child, null));
            Map<String, Object> attributes = Maps.newHashMap();
            attributes.put("value", child);
            attributes.put("null", null);
            assertThat(actual.getField("attributes")).isEqualTo(attributes);
          } else {
            assertThat(actual.getField("nested")).isNull();
            assertThat(actual.getField("items")).isNull();
            assertThat(actual.getField("attributes")).isNull();
          }

          index += 1;
        }

        assertThat(index).isEqualTo(expected.size());
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void internalReaderUsesExpectedTimestampUnit(boolean dictionary, boolean nanos)
      throws IOException {
    BigInteger scale = BigInteger.valueOf(nanos ? 1 : 1_000);
    List<BigInteger> values =
        List.of(
            BigInteger.valueOf(Long.MIN_VALUE).multiply(scale),
            BigInteger.valueOf(Long.MAX_VALUE).multiply(scale),
            BigInteger.valueOf(-1_001),
            BigInteger.valueOf(1_001));
    List<BigInteger> expected = Lists.newArrayList();
    for (int index = 0; index < 32; index += 1) {
      expected.add(values.get(index % values.size()));
    }

    File file = Int96TestUtil.write(temp.resolve("internal.parquet"), dictionary, expected);
    Schema schema = schema(nanos).select("id", "ts");
    try (CloseableIterable<Record> records =
        Parquet.read(Files.localInput(file))
            .project(schema)
            .createReaderFunc(fileSchema -> InternalReader.create(schema, fileSchema))
            .build()) {
      int index = 0;
      for (Record actual : records) {
        assertThat(actual.getField("ts"))
            .isEqualTo(Int96TestUtil.expectedTicks(expected.get(index), nanos));
        index += 1;
      }

      assertThat(index).isEqualTo(expected.size());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void truncatesSubmicrosecondValues(boolean dictionary) throws IOException {
    for (long value : new long[] {-1_001, -1, 1, 1_001}) {
      List<BigInteger> values = prefixThen(BigInteger.valueOf(value));
      File file =
          Int96TestUtil.write(temp.resolve("inexact-" + value + ".parquet"), dictionary, values);
      Schema schema = schema(false).select("id", "ts");
      try (CloseableIterable<Record> records =
          Parquet.read(Files.localInput(file))
              .project(schema)
              .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
              .build()) {
        assertThat(records)
            .extracting(record -> record.getField("ts"))
            .containsExactlyElementsOf(
                values.stream().map(nanos -> expectedTimestamp(nanos, false, true)).toList());
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void rejectsValuesOutsideTargetRange(boolean dictionary, boolean nanos) throws IOException {
    BigInteger scale = BigInteger.valueOf(nanos ? 1 : 1_000);
    for (BigInteger outside :
        List.of(
            BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE),
            BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE))) {
      File file =
          Int96TestUtil.write(
              temp.resolve("outside-" + outside + ".parquet"),
              dictionary,
              prefixThen(outside.multiply(scale)));
      assertThatThrownBy(() -> consume(file, schema(nanos)))
          .isInstanceOf(ArithmeticException.class)
          .hasMessageContaining("overflow");
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void rejectsUnrepresentableNestedTimestamp(boolean dictionary, boolean nanos) throws IOException {
    BigInteger invalid =
        BigInteger.valueOf(Long.MAX_VALUE)
            .add(BigInteger.ONE)
            .multiply(BigInteger.valueOf(nanos ? 1 : 1000));
    List<Row> rows = Lists.newArrayList();
    for (int index = 0; index < 32; index += 1) {
      rows.add(new Row(index, null, true, index < 16 ? BigInteger.ZERO : invalid));
    }

    File file = Int96TestUtil.writeRows(temp.resolve("nested.parquet"), dictionary, rows);
    for (String field : List.of("nested", "items", "attributes")) {
      assertThatThrownBy(() -> consume(file, schema(nanos).select("id", field)))
          .as("Unrepresentable timestamp in %s", field)
          .isInstanceOf(ArithmeticException.class)
          .hasMessageContaining("overflow");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void projectionDoesNotDecodeUnselectedTimestamps(boolean dictionary) throws IOException {
    File file =
        Int96TestUtil.write(
            temp.resolve("projection.parquet"),
            dictionary,
            prefixThen(
                BigInteger.valueOf(Long.MAX_VALUE)
                    .add(BigInteger.ONE)
                    .multiply(BigInteger.valueOf(1000))));
    Schema projection = schema(false).select("id");
    try (CloseableIterable<Record> records =
        Parquet.read(Files.localInput(file))
            .project(projection)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(projection, fileSchema))
            .build()) {
      int count = 0;
      for (Record record : records) {
        assertThat(record.getField("id")).isEqualTo(count);
        count += 1;
      }

      assertThat(count).isEqualTo(32);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void prunedRowGroupDoesNotDecodeUnrepresentableTimestamp(boolean dictionary) throws IOException {
    File file =
        Int96TestUtil.write(
            temp.resolve("pruned.parquet"),
            dictionary,
            prefixThen(
                BigInteger.valueOf(Long.MAX_VALUE)
                    .add(BigInteger.ONE)
                    .multiply(BigInteger.valueOf(1000))));
    Schema schema = schema(false).select("id", "ts");
    try (CloseableIterable<Record> records =
        Parquet.read(Files.localInput(file))
            .project(schema)
            .filter(Expressions.lessThan("id", 16))
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      int count = 0;
      for (Record record : records) {
        assertThat(record.getField("id")).isEqualTo(count);
        assertThat(record.getField("ts"))
            .isEqualTo(expectedTimestamp(BigInteger.ZERO, false, true));
        count += 1;
      }

      assertThat(count).isEqualTo(16);
    }
  }

  private static List<BigInteger> prefixThen(BigInteger value) {
    List<BigInteger> rows = Lists.newArrayList();
    for (int index = 0; index < 32; index += 1) {
      rows.add(index < 16 ? BigInteger.ZERO : value);
    }

    return rows;
  }

  private static void consume(File file, Schema schema) throws IOException {
    try (CloseableIterable<Record> records =
        Parquet.read(Files.localInput(file))
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      for (Record record : records) {
        assertThat(record).isNotNull();
      }
    }
  }

  private static Schema schema(boolean nanos) {
    return schema(nanos, true);
  }

  private static Schema schema(boolean nanos, boolean zoned) {
    Type timestamp =
        nanos
            ? (zoned ? Types.TimestampNanoType.withZone() : Types.TimestampNanoType.withoutZone())
            : (zoned ? Types.TimestampType.withZone() : Types.TimestampType.withoutZone());
    return new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "ts", timestamp),
        Types.NestedField.optional(
            3, "nested", Types.StructType.of(Types.NestedField.optional(4, "ts", timestamp))),
        Types.NestedField.optional(5, "items", Types.ListType.ofOptional(6, timestamp)),
        Types.NestedField.optional(
            7, "attributes", Types.MapType.ofOptional(8, 9, Types.StringType.get(), timestamp)));
  }

  private static Object expectedTimestamp(BigInteger epochNanos, boolean nanos, boolean zoned) {
    if (epochNanos == null) {
      return null;
    }

    BigInteger truncatedNanos =
        BigInteger.valueOf(Int96TestUtil.expectedTicks(epochNanos, nanos))
            .multiply(BigInteger.valueOf(nanos ? 1 : 1000));
    BigInteger[] secondsAndNanos = truncatedNanos.divideAndRemainder(NANOS_PER_SECOND);
    OffsetDateTime timestamp =
        Instant.ofEpochSecond(
                secondsAndNanos[0].longValueExact(), secondsAndNanos[1].longValueExact())
            .atOffset(ZoneOffset.UTC);
    return zoned ? timestamp : timestamp.toLocalDateTime();
  }
}

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
package org.apache.iceberg.spark.data;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Int96TestUtil;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestSparkParquetInt96 {
  private static final int ROW_COUNT = 32;
  private static final BigInteger NANOS_PER_MICRO = BigInteger.valueOf(1000);
  private static final BigInteger OVERFLOW_NANOS =
      BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE).multiply(NANOS_PER_MICRO);
  private static final Schema TIMESTAMP_SCHEMA =
      new Schema(optional(2, "ts", Types.TimestampType.withZone()));
  private static final Schema ID_SCHEMA = new Schema(required(1, "id", Types.IntegerType.get()));
  private static final Schema NESTED_SCHEMA =
      new Schema(
          optional(
              3, "nested", Types.StructType.of(optional(4, "ts", Types.TimestampType.withZone()))),
          optional(5, "items", Types.ListType.ofOptional(6, Types.TimestampType.withZone())),
          optional(
              7,
              "attributes",
              Types.MapType.ofOptional(
                  8, 9, Types.StringType.get(), Types.TimestampType.withZone())));

  @TempDir private Path temp;

  @ParameterizedTest
  @MethodSource("readModes")
  void readsExactMicroseconds(boolean vectorized, boolean dictionary) throws IOException {
    List<Long> values =
        Arrays.asList(
            null, 0L, 1L, -1L, 1_000_000L, -86_400_000_000L, Long.MIN_VALUE, Long.MAX_VALUE);
    List<Long> expected =
        IntStream.range(0, ROW_COUNT).mapToObj(index -> values.get(index % values.size())).toList();
    List<BigInteger> nanos =
        expected.stream()
            .map(
                value -> value == null ? null : BigInteger.valueOf(value).multiply(NANOS_PER_MICRO))
            .toList();
    File file = Int96TestUtil.write(temp.resolve("exact.parquet"), dictionary, nanos);

    assertThat(read(file, TIMESTAMP_SCHEMA, vectorized)).containsExactlyElementsOf(expected);
    Schema withoutZone = new Schema(optional(2, "ts", Types.TimestampType.withoutZone()));
    assertThat(read(file, withoutZone, vectorized)).containsExactlyElementsOf(expected);
  }

  @ParameterizedTest
  @MethodSource("subMicrosecondValues")
  void truncatesSubMicrosecondValues(boolean vectorized, boolean dictionary, BigInteger nanos)
      throws IOException {
    File file =
        Int96TestUtil.write(
            temp.resolve("sub-microsecond.parquet"),
            dictionary,
            Collections.nCopies(ROW_COUNT, nanos));

    assertThat(read(file, TIMESTAMP_SCHEMA, vectorized))
        .containsExactlyElementsOf(
            Collections.nCopies(ROW_COUNT, Int96TestUtil.expectedTicks(nanos, false)));
  }

  @ParameterizedTest
  @MethodSource("overflowingValues")
  void rejectsMicrosecondOverflow(boolean vectorized, boolean dictionary, BigInteger nanos)
      throws IOException {
    File file =
        Int96TestUtil.write(
            temp.resolve("overflow.parquet"), dictionary, Collections.nCopies(ROW_COUNT, nanos));

    assertThatThrownBy(() -> read(file, TIMESTAMP_SCHEMA, vectorized))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("overflow");
  }

  @ParameterizedTest
  @MethodSource("readModes")
  void rejectsAnInvalidTimestampAfterAValidRowGroup(boolean vectorized, boolean dictionary)
      throws IOException {
    List<BigInteger> nanos =
        Lists.newArrayList(Collections.nCopies(ROW_COUNT / 2, BigInteger.ZERO));
    nanos.addAll(Collections.nCopies(ROW_COUNT / 2, OVERFLOW_NANOS));
    File file = Int96TestUtil.write(temp.resolve("later-row-group.parquet"), dictionary, nanos);

    assertThatThrownBy(() -> read(file, TIMESTAMP_SCHEMA, vectorized))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("overflow");
  }

  @ParameterizedTest
  @MethodSource("readModes")
  void doesNotDecodeUnprojectedTimestamps(boolean vectorized, boolean dictionary)
      throws IOException {
    File file =
        Int96TestUtil.write(
            temp.resolve("projection.parquet"),
            dictionary,
            Collections.nCopies(ROW_COUNT, OVERFLOW_NANOS));

    assertThat(read(file, ID_SCHEMA, vectorized))
        .containsExactlyElementsOf(
            IntStream.range(0, ROW_COUNT).mapToObj(index -> (long) index).toList());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void readsNestedTimestampsAndNulls(boolean dictionary) throws IOException {
    List<Int96TestUtil.Row> rows = Lists.newArrayList();
    List<Long> expected = Lists.newArrayList();
    for (int index = 0; index < ROW_COUNT; index += 1) {
      Long micros = index % 4 < 2 ? null : (index % 4 == 2 ? 1L : -1L);
      rows.add(
          new Int96TestUtil.Row(
              index,
              null,
              index % 4 != 0,
              micros == null ? null : BigInteger.valueOf(micros).multiply(NANOS_PER_MICRO)));
      expected.add(micros);
    }
    File file = Int96TestUtil.writeRows(temp.resolve("nested.parquet"), dictionary, rows);

    try (CloseableIterable<InternalRow> readRows =
        Parquet.read(Files.localInput(file))
            .project(NESTED_SCHEMA)
            .createReaderFunc(type -> SparkParquetReaders.buildReader(NESTED_SCHEMA, type))
            .build()) {
      int index = 0;
      for (InternalRow row : readRows) {
        if (!rows.get(index).parentPresent()) {
          assertThat(row.isNullAt(0)).isTrue();
          assertThat(row.isNullAt(1)).isTrue();
          assertThat(row.isNullAt(2)).isTrue();
        } else {
          InternalRow struct = row.getStruct(0, 1);
          ArrayData items = row.getArray(1);
          MapData attributes = row.getMap(2);
          assertThat(struct.isNullAt(0) ? null : struct.getLong(0)).isEqualTo(expected.get(index));
          assertThat(items.numElements()).isEqualTo(2);
          assertThat(items.isNullAt(0) ? null : items.getLong(0)).isEqualTo(expected.get(index));
          assertThat(items.isNullAt(1)).isTrue();
          assertThat(attributes.numElements()).isEqualTo(2);
          assertThat(attributes.keyArray().getUTF8String(0).toString()).isEqualTo("value");
          assertThat(attributes.keyArray().getUTF8String(1).toString()).isEqualTo("null");
          ArrayData mapValues = attributes.valueArray();
          assertThat(mapValues.isNullAt(0) ? null : mapValues.getLong(0))
              .isEqualTo(expected.get(index));
          assertThat(mapValues.isNullAt(1)).isTrue();
        }
        index += 1;
      }
      assertThat(index).isEqualTo(ROW_COUNT);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsNestedOverflow(boolean dictionary) throws IOException {
    List<Int96TestUtil.Row> rows =
        IntStream.range(0, ROW_COUNT)
            .mapToObj(index -> new Int96TestUtil.Row(index, null, true, OVERFLOW_NANOS))
            .toList();
    File file = Int96TestUtil.writeRows(temp.resolve("nested-overflow.parquet"), dictionary, rows);

    assertThatThrownBy(() -> read(file, NESTED_SCHEMA, false))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("overflow");
  }

  private List<Long> read(File file, Schema schema, boolean vectorized) throws IOException {
    List<Long> values = Lists.newArrayList();
    boolean idOnly = schema.columns().get(0).type().typeId() == TypeID.INTEGER;
    if (vectorized) {
      try (RootAllocator allocator = new RootAllocator();
          CloseableIterable<ColumnarBatch> batches =
              Parquet.read(Files.localInput(file))
                  .project(schema)
                  .recordsPerBatch(5)
                  .createBatchedReaderFunc(
                      type ->
                          VectorizedSparkParquetReaders.buildReader(
                              schema, type, Map.of(), allocator))
                  .build()) {
        for (ColumnarBatch batch : batches) {
          for (int index = 0; index < batch.numRows(); index += 1) {
            values.add(
                batch.column(0).isNullAt(index)
                    ? null
                    : idOnly
                        ? (long) batch.column(0).getInt(index)
                        : batch.column(0).getLong(index));
          }
        }
      }
    } else {
      try (CloseableIterable<InternalRow> rows =
          Parquet.read(Files.localInput(file))
              .project(schema)
              .createReaderFunc(type -> SparkParquetReaders.buildReader(schema, type))
              .build()) {
        for (InternalRow row : rows) {
          InternalRow value =
              schema.columns().get(0).type().isStructType() && !row.isNullAt(0)
                  ? row.getStruct(0, 1)
                  : row;
          values.add(value.isNullAt(0) ? null : idOnly ? (long) value.getInt(0) : value.getLong(0));
        }
      }
    }
    return values;
  }

  private static Stream<Arguments> readModes() {
    return Stream.of(
        Arguments.of(false, false),
        Arguments.of(false, true),
        Arguments.of(true, false),
        Arguments.of(true, true));
  }

  private static Stream<Arguments> subMicrosecondValues() {
    return readModes()
        .flatMap(
            mode ->
                Stream.of(1L, -1L, 1001L, -1001L)
                    .map(
                        value ->
                            Arguments.of(mode.get()[0], mode.get()[1], BigInteger.valueOf(value))));
  }

  private static Stream<Arguments> overflowingValues() {
    return readModes()
        .flatMap(
            mode ->
                Stream.of(
                        BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE),
                        BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE))
                    .map(
                        value ->
                            Arguments.of(
                                mode.get()[0], mode.get()[1], value.multiply(NANOS_PER_MICRO))));
  }
}

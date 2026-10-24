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
package org.apache.iceberg.arrow.vectorized;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Int96TestUtil;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestArrowInt96 {
  @TempDir private Path temp;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void readsMixedDictionaryAndPlainTimestamps(boolean nanos) throws IOException {
    List<BigInteger> raw = mixedValues(nanos, BigInteger.valueOf(-1));
    File file = Int96TestUtil.writeMixedEncoding(temp.resolve("mixed-exact.parquet"), raw);
    List<Long> actual = Lists.newArrayList();
    readInto(file, schema(nanos, true), actual);

    assertThat(actual)
        .containsExactlyElementsOf(
            raw.stream().map(value -> Int96TestUtil.expectedTicks(value, nanos)).toList());
  }

  @Test
  void truncatesSubMicrosecondsInAMixedDictionaryColumn() throws IOException {
    List<BigInteger> raw = mixedValues(false, BigInteger.ZERO);
    Collections.fill(raw.subList(0, 12), BigInteger.valueOf(-1001));
    File file = Int96TestUtil.writeMixedEncoding(temp.resolve("mixed-precision.parquet"), raw);

    List<Long> actual = Lists.newArrayList();
    readInto(file, schema(false, true), actual);
    assertThat(actual)
        .containsExactlyElementsOf(
            raw.stream().map(value -> Int96TestUtil.expectedTicks(value, false)).toList());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsOverflowInAMixedDictionaryColumn(boolean nanos) throws IOException {
    BigInteger invalid = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
    List<BigInteger> raw = mixedValues(nanos, invalid);
    File file = Int96TestUtil.writeMixedEncoding(temp.resolve("mixed-overflow.parquet"), raw);

    assertThatThrownBy(() -> readInto(file, schema(nanos, true), Lists.newArrayList()))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("overflow");
  }

  private static List<BigInteger> mixedValues(boolean nanos, BigInteger firstValue) {
    BigInteger scale = nanos ? BigInteger.ONE : BigInteger.valueOf(1000);
    List<BigInteger> raw = Lists.newArrayList(Collections.nCopies(12, firstValue.multiply(scale)));
    for (int index = 0; index < 36; index += 1) {
      raw.add(BigInteger.valueOf(1001L + index).multiply(scale));
    }
    return raw;
  }

  @ParameterizedTest
  @MethodSource("unitsAndEncodings")
  void readsExactValuesAndNulls(boolean nanos, boolean dictionary) throws IOException {
    long[] ticks = {0, 1, -1, 1_234_567_890L, -86_400_000_000L, Long.MIN_VALUE, Long.MAX_VALUE};
    List<BigInteger> raw = Lists.newArrayList();
    List<Long> expected = Lists.newArrayList();
    for (int index = 0; index < 32; index += 1) {
      Long value = index % 9 == 0 ? null : ticks[index % ticks.length];
      expected.add(value);
      raw.add(
          value == null
              ? null
              : BigInteger.valueOf(value)
                  .multiply(nanos ? BigInteger.ONE : BigInteger.valueOf(1000)));
    }
    File file = Int96TestUtil.write(temp.resolve("exact.parquet"), dictionary, raw);
    List<Long> actual = Lists.newArrayList();
    readInto(file, schema(nanos, true), actual);
    assertThat(actual).containsExactlyElementsOf(expected);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void readsTimestampsWithoutZone(boolean nanos) throws IOException {
    BigInteger raw = BigInteger.valueOf(nanos ? -1 : -1000);
    File file =
        Int96TestUtil.write(temp.resolve("local.parquet"), false, Collections.nCopies(16, raw));
    List<Long> actual = Lists.newArrayList();
    readInto(file, schema(nanos, false), actual);
    assertThat(actual).containsExactlyElementsOf(Collections.nCopies(16, -1L));
  }

  @ParameterizedTest
  @MethodSource("inexactValues")
  void truncatesSubMicrosecondPrecision(boolean dictionary, long nanos) throws IOException {
    File file =
        Int96TestUtil.write(
            temp.resolve("precision.parquet"),
            dictionary,
            Collections.nCopies(16, BigInteger.valueOf(nanos)));
    List<Long> actual = Lists.newArrayList();
    readInto(file, schema(false, true), actual);
    assertThat(actual)
        .containsExactlyElementsOf(Collections.nCopies(16, Math.floorDiv(nanos, 1000)));
  }

  @ParameterizedTest
  @MethodSource("overflowingValues")
  void rejectsOverflowAfterAValidRowGroup(boolean nanos, boolean dictionary, boolean upper)
      throws IOException {
    BigInteger ticks =
        upper
            ? BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)
            : BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE);
    BigInteger invalid = ticks.multiply(nanos ? BigInteger.ONE : BigInteger.valueOf(1000));
    List<BigInteger> raw = Lists.newArrayList(Collections.nCopies(16, BigInteger.ZERO));
    raw.addAll(Collections.nCopies(16, invalid));
    File file = Int96TestUtil.write(temp.resolve("overflow.parquet"), dictionary, raw);
    List<Long> prefix = Lists.newArrayList();
    assertThatThrownBy(() -> readInto(file, schema(nanos, true), prefix))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("overflow");
    assertThat(prefix).containsExactlyElementsOf(Collections.nCopies(16, 0L));
  }

  private static Schema schema(boolean nanos, boolean zoned) {
    return new Schema(
        Types.NestedField.optional(
            2,
            "ts",
            nanos
                ? (zoned
                    ? Types.TimestampNanoType.withZone()
                    : Types.TimestampNanoType.withoutZone())
                : (zoned ? Types.TimestampType.withZone() : Types.TimestampType.withoutZone())));
  }

  private static void readInto(File file, Schema schema, List<Long> values) throws IOException {
    try (CloseableIterable<ColumnarBatch> batches =
        Parquet.read(Files.localInput(file))
            .project(schema)
            .recordsPerBatch(4)
            .createBatchedReaderFunc(
                fileSchema ->
                    (ArrowBatchReader)
                        TypeWithSchemaVisitor.visit(
                            schema.asStruct(),
                            fileSchema,
                            new VectorizedReaderBuilder(
                                schema, fileSchema, true, Map.of(), ArrowBatchReader::new)))
            .build()) {
      for (ColumnarBatch batch : batches) {
        ColumnVector column = batch.column(0);
        for (int row = 0; row < batch.numRows(); row += 1) {
          values.add(column.isNullAt(row) ? null : column.getLong(row));
        }
        TimeStampVector decoded = (TimeStampVector) column.getArrowVector();
        try {
          for (int row = 0; row < batch.numRows(); row += 1) {
            if (!column.isNullAt(row)) {
              assertThat(decoded.get(row)).isEqualTo(column.getLong(row));
            } else {
              assertThat(decoded.isNull(row)).isTrue();
            }
          }
        } finally {
          if (decoded != column.getFieldVector()) {
            decoded.close();
          }
        }
      }
    }
  }

  private static Stream<Arguments> unitsAndEncodings() {
    return Stream.of(
        Arguments.of(false, false),
        Arguments.of(false, true),
        Arguments.of(true, false),
        Arguments.of(true, true));
  }

  private static Stream<Arguments> inexactValues() {
    return Stream.of(false, true)
        .flatMap(
            dictionary ->
                Stream.of(1L, -1L, 1001L, -1001L).map(nanos -> Arguments.of(dictionary, nanos)));
  }

  private static Stream<Arguments> overflowingValues() {
    return unitsAndEncodings()
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .map(upper -> Arguments.of(mode.get()[0], mode.get()[1], upper)));
  }
}

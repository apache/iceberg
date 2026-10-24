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
package org.apache.iceberg.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Int96TestUtil;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestIcebergGenericsInt96 {
  private static final int ROW_COUNT = 32;

  @TempDir private Path temp;

  @AfterEach
  void clearTables() {
    TestTables.clearTables();
  }

  @ParameterizedTest
  @MethodSource("timestampModes")
  void filtersOnUnselectedTimestamp(boolean nanos, boolean zoned, boolean dictionary)
      throws Exception {
    Table table = tableWithTimestamps(nanos, zoned, dictionary, timestampValues(nanos));
    String bound =
        (nanos ? "1970-01-01T00:00:00.000001001" : "1970-01-01T00:00:00.001001")
            + (zoned ? "+00:00" : "");

    try (CloseableIterable<Record> rows =
        IcebergGenerics.read(table)
            .select("id")
            .where(Expressions.greaterThanOrEqual("ts", bound))
            .build()) {
      assertThat(rows)
          .extracting(row -> row.getField("id"))
          .containsExactlyElementsOf(
              IntStream.range(0, ROW_COUNT).filter(id -> id % 4 >= 2).boxed().toList());
    }
  }

  @ParameterizedTest
  @MethodSource("timestampModes")
  void appliesEqualityDeletesOnUnselectedTimestamp(boolean nanos, boolean zoned, boolean dictionary)
      throws Exception {
    Table table = tableWithTimestamps(nanos, zoned, dictionary, timestampValues(nanos));
    Schema deleteSchema = table.schema().select("ts");
    GenericRecord delete = GenericRecord.create(deleteSchema);
    delete.setField(
        "ts",
        nanos
            ? (zoned
                ? DateTimeUtil.timestamptzFromNanos(1001)
                : DateTimeUtil.timestampFromNanos(1001))
            : (zoned
                ? DateTimeUtil.timestamptzFromMicros(1001)
                : DateTimeUtil.timestampFromMicros(1001)));
    EqualityDeleteWriter<Record> writer =
        Parquet.writeDeletes(Files.localOutput(temp.resolve("deletes.parquet").toFile()))
            .createWriterFunc(GenericParquetWriter::create)
            .rowSchema(deleteSchema)
            .withSpec(table.spec())
            .equalityFieldIds(2)
            .buildEqualityWriter();
    try (writer) {
      writer.write(delete);
    }
    table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();

    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).select("id").build()) {
      assertThat(rows)
          .extracting(row -> row.getField("id"))
          .containsExactlyElementsOf(
              IntStream.range(0, ROW_COUNT).filter(id -> id % 4 != 2).boxed().toList());
    }
  }

  @ParameterizedTest
  @MethodSource("timestampModes")
  void readsInt96EqualityDeleteKeys(boolean nanos, boolean zoned, boolean dictionary)
      throws Exception {
    Table table = tableWithTimestamps(nanos, zoned, dictionary, timestampValues(nanos));
    File deletes =
        Int96TestUtil.write(
            temp.resolve("int96-deletes.parquet"),
            dictionary,
            Collections.nCopies(16, BigInteger.valueOf(-1001L * (nanos ? 1 : 1000))));
    table
        .newRowDelta()
        .addDeletes(
            FileMetadata.deleteFileBuilder(table.spec())
                .ofEqualityDeletes(2)
                .withPath(deletes.getAbsolutePath())
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(deletes.length())
                .withRecordCount(16)
                .build())
        .commit();

    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).select("id").build()) {
      assertThat(rows)
          .extracting(row -> row.getField("id"))
          .containsExactlyElementsOf(
              IntStream.range(0, ROW_COUNT).filter(id -> id % 4 != 0).boxed().toList());
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void rejectsInvalidTimestampRequiredByResidual(boolean nanos, boolean dictionary)
      throws Exception {
    BigInteger invalid =
        BigInteger.valueOf(Long.MAX_VALUE)
            .add(BigInteger.ONE)
            .multiply(BigInteger.valueOf(nanos ? 1 : 1000));
    Table table = tableWithTimestamp(nanos, dictionary, invalid);

    assertThatThrownBy(
            () -> {
              try (CloseableIterable<Record> rows =
                  IcebergGenerics.read(table)
                      .select("id")
                      .where(Expressions.greaterThanOrEqual("ts", 0L))
                      .build()) {
                rows.iterator().hasNext();
              }
            })
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("overflow");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void preservesPruningWithoutReadingUnselectedTimestamp(boolean dictionary) throws Exception {
    Table table =
        tableWithTimestamp(
            false,
            dictionary,
            BigInteger.valueOf(Long.MAX_VALUE)
                .add(BigInteger.ONE)
                .multiply(BigInteger.valueOf(1000)));

    try (CloseableIterable<Record> rows =
        IcebergGenerics.read(table).select("id").where(Expressions.lessThan("id", 0)).build()) {
      assertThat(rows).isEmpty();
    }
  }

  private Table tableWithTimestamp(boolean nanos, boolean dictionary, BigInteger timestamp)
      throws Exception {
    return tableWithTimestamps(nanos, true, dictionary, Collections.nCopies(ROW_COUNT, timestamp));
  }

  private Table tableWithTimestamps(
      boolean nanos, boolean zoned, boolean dictionary, List<BigInteger> timestamps)
      throws Exception {
    File file = Int96TestUtil.write(temp.resolve("timestamps.parquet"), dictionary, timestamps);
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(
                2,
                "ts",
                nanos
                    ? (zoned
                        ? Types.TimestampNanoType.withZone()
                        : Types.TimestampNanoType.withoutZone())
                    : (zoned
                        ? Types.TimestampType.withZone()
                        : Types.TimestampType.withoutZone())));
    Table table =
        TestTables.create(
            temp.resolve("table").toFile(), "timestamps", schema, PartitionSpec.unpartitioned(), 3);
    table
        .newAppend()
        .appendFile(
            DataFiles.builder(table.spec())
                .withPath(file.getAbsolutePath())
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(file.length())
                .withRecordCount(timestamps.size())
                .build())
        .commit();
    return table;
  }

  private static List<BigInteger> timestampValues(boolean nanos) {
    long[] ticks = {-1001, 0, 1001, 2002};
    return IntStream.range(0, ROW_COUNT)
        .mapToObj(id -> BigInteger.valueOf(ticks[id % ticks.length] * (nanos ? 1 : 1000)))
        .toList();
  }

  private static Stream<Arguments> timestampModes() {
    return Stream.of(false, true)
        .flatMap(
            nanos ->
                Stream.of(false, true)
                    .flatMap(
                        zoned ->
                            Stream.of(false, true)
                                .map(dictionary -> Arguments.of(nanos, zoned, dictionary))));
  }
}

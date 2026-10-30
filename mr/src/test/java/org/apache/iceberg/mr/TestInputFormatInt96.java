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
package org.apache.iceberg.mr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.TestIcebergInputFormats.TestInputFormat;
import org.apache.iceberg.parquet.Int96TestUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestInputFormatInt96 {
  private static final int ROW_COUNT = 32;

  @TempDir private Path temp;

  @ParameterizedTest
  @MethodSource("readingModes")
  void preservesExactMicroseconds(TestInputFormat.Factory<Record> inputFormat, boolean dictionary)
      throws Exception {
    Schema schema = schema(false);
    List<BigInteger> values = Collections.nCopies(ROW_COUNT, BigInteger.valueOf(-1_234_567_000L));

    assertThat(readRecords(inputFormat, dictionary, schema, values))
        .containsExactlyElementsOf(expectedRecords(schema, values));
  }

  @ParameterizedTest
  @MethodSource("readingModes")
  void preservesNanoseconds(TestInputFormat.Factory<Record> inputFormat, boolean dictionary)
      throws Exception {
    Schema schema = schema(true);
    List<BigInteger> values = Collections.nCopies(ROW_COUNT, BigInteger.valueOf(-1));

    assertThat(readRecords(inputFormat, dictionary, schema, values))
        .containsExactlyElementsOf(expectedRecords(schema, values));
  }

  @ParameterizedTest
  @MethodSource("readingModes")
  void truncatesSubMicrosecondsAfterValidRows(
      TestInputFormat.Factory<Record> inputFormat, boolean dictionary) throws Exception {
    List<BigInteger> values = valuesEndingWith(BigInteger.valueOf(-1));

    assertThat(readRecords(inputFormat, dictionary, schema(false), values))
        .containsExactlyElementsOf(
            expectedRecords(schema(false), valuesEndingWith(BigInteger.valueOf(-1000))));
  }

  @ParameterizedTest
  @MethodSource("readingModes")
  void rejectsNanosecondOverflowAfterValidRows(
      TestInputFormat.Factory<Record> inputFormat, boolean dictionary) throws Exception {
    List<BigInteger> values =
        valuesEndingWith(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));

    assertThatThrownBy(() -> readRecords(inputFormat, dictionary, schema(true), values))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("overflow");
  }

  private List<Record> readRecords(
      TestInputFormat.Factory<Record> inputFormat,
      boolean dictionary,
      Schema schema,
      List<BigInteger> values)
      throws Exception {
    File file = Int96TestUtil.write(temp.resolve("timestamps.parquet"), dictionary, values);
    Configuration conf = new Configuration();
    conf.set(CatalogUtil.ICEBERG_CATALOG_TYPE, Catalogs.LOCATION);
    Table table =
        new HadoopTables(conf)
            .buildTable(temp.resolve("table").toString(), schema)
            .withPartitionSpec(PartitionSpec.unpartitioned())
            .withProperty(TableProperties.FORMAT_VERSION, "3")
            .create();
    table
        .newAppend()
        .appendFile(
            DataFiles.builder(table.spec())
                .withPath(file.getAbsolutePath())
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(file.length())
                .withRecordCount(values.size())
                .build())
        .commit();
    Configuration readConf =
        new InputFormatConfig.ConfigBuilder(conf)
            .readFrom(table.location())
            .schema(table.schema())
            .project(schema)
            .conf();
    return inputFormat.create(readConf).getRecords();
  }

  private static List<Record> expectedRecords(Schema schema, List<BigInteger> values) {
    List<Record> expected = Lists.newArrayList();
    for (int index = 0; index < values.size(); index += 1) {
      GenericRecord record = GenericRecord.create(schema);
      record.setField("id", index);
      record.setField(
          "ts",
          Instant.EPOCH.plusNanos(values.get(index).longValueExact()).atOffset(ZoneOffset.UTC));
      expected.add(record);
    }

    return expected;
  }

  private static List<BigInteger> valuesEndingWith(BigInteger value) {
    List<BigInteger> values = Lists.newArrayList(Collections.nCopies(ROW_COUNT, BigInteger.ZERO));
    values.set(ROW_COUNT - 1, value);
    return values;
  }

  private static Schema schema(boolean nanos) {
    return new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(
            2, "ts", nanos ? Types.TimestampNanoType.withZone() : Types.TimestampType.withZone()));
  }

  private static Stream<Arguments> readingModes() {
    return TestIcebergInputFormats.TESTED_INPUT_FORMATS.stream()
        .flatMap(
            inputFormat ->
                Stream.of(false, true).map(dictionary -> Arguments.of(inputFormat, dictionary)));
  }
}

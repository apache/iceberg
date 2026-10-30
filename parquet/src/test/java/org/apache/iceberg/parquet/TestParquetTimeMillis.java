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

import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.util.DateTimeUtil.microsFromTime;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestParquetTimeMillis {
  private static final LocalTime LOWER_TIME = LocalTime.of(12, 34, 56, 123_000_000);
  private static final LocalTime UPPER_TIME = LocalTime.of(23, 59, 59, 999_000_000);

  @TempDir private File temp;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void rowGroupFilterUsesMicros(boolean isAdjustedToUTC) throws IOException {
    File file = writeFile(isAdjustedToUTC);
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      MessageType fileSchema = reader.getFileMetaData().getSchema();
      Schema schema = ParquetSchemaUtil.convert(fileSchema);
      BlockMetaData rowGroup = reader.getRowGroups().get(0);

      assertThat(
              new ParquetMetricsRowGroupFilter(schema, equal("time", microsFromTime(LOWER_TIME)))
                  .shouldRead(fileSchema, rowGroup))
          .isTrue();
      assertThat(
              new ParquetMetricsRowGroupFilter(schema, equal("time", microsFromTime(UPPER_TIME)))
                  .shouldRead(fileSchema, rowGroup))
          .isTrue();
      assertThat(
              new ParquetMetricsRowGroupFilter(schema, lessThan("time", microsFromTime(LOWER_TIME)))
                  .shouldRead(fileSchema, rowGroup))
          .isFalse();
      assertThat(
              new ParquetMetricsRowGroupFilter(
                      schema, greaterThan("time", microsFromTime(UPPER_TIME)))
                  .shouldRead(fileSchema, rowGroup))
          .isFalse();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void dictionaryFilterUsesMicros(boolean isAdjustedToUTC) throws IOException {
    File file = writeFile(isAdjustedToUTC);
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      MessageType fileSchema = reader.getFileMetaData().getSchema();
      Schema schema = ParquetSchemaUtil.convert(fileSchema);
      BlockMetaData rowGroup = reader.getRowGroups().get(0);
      DictionaryPageReadStore dictionaries = reader.getNextDictionaryReader();

      assertThat(
              new ParquetDictionaryRowGroupFilter(schema, equal("time", microsFromTime(LOWER_TIME)))
                  .shouldRead(fileSchema, rowGroup, dictionaries))
          .isTrue();
      assertThat(
              new ParquetDictionaryRowGroupFilter(schema, equal("time", microsFromTime(UPPER_TIME)))
                  .shouldRead(fileSchema, rowGroup, dictionaries))
          .isTrue();
      assertThat(
              new ParquetDictionaryRowGroupFilter(
                      schema, lessThan("time", microsFromTime(LOWER_TIME)))
                  .shouldRead(fileSchema, rowGroup, dictionaries))
          .isFalse();
      assertThat(
              new ParquetDictionaryRowGroupFilter(
                      schema, greaterThan("time", microsFromTime(UPPER_TIME)))
                  .shouldRead(fileSchema, rowGroup, dictionaries))
          .isFalse();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void fileMetricsUseMicros(boolean isAdjustedToUTC) throws IOException {
    File file = writeFile(isAdjustedToUTC);
    Metrics metrics = ParquetUtil.fileMetrics(Files.localInput(file), MetricsConfig.getDefault());

    assertThat(metrics.lowerBounds())
        .containsEntry(1, Conversions.toByteBuffer(TimeType.get(), microsFromTime(LOWER_TIME)));
    assertThat(metrics.upperBounds())
        .containsEntry(1, Conversions.toByteBuffer(TimeType.get(), microsFromTime(UPPER_TIME)));
  }

  private File writeFile(boolean isAdjustedToUTC) throws IOException {
    MessageType writeSchema =
        new MessageType(
            "test",
            Types.required(PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.timeType(isAdjustedToUTC, MILLIS))
                .id(1)
                .named("time"));
    File file = new File(temp, "times.parquet");
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(ParquetIO.file(Files.localOutput(file)))
            .withType(writeSchema)
            .withDictionaryEncoding(true)
            .build()) {
      SimpleGroupFactory groups = new SimpleGroupFactory(writeSchema);
      // Repeat values to prevent dictionary encoding fallback.
      for (int copy = 0; copy < 2; copy += 1) {
        writer.write(groups.newGroup().append("time", LOWER_TIME.get(ChronoField.MILLI_OF_DAY)));
        writer.write(groups.newGroup().append("time", UPPER_TIME.get(ChronoField.MILLI_OF_DAY)));
      }
    }

    return file;
  }
}

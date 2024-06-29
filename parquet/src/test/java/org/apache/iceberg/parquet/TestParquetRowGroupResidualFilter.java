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

import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
import static org.apache.iceberg.TableProperties.PARQUET_DICT_SIZE_BYTES;
import static org.apache.iceberg.avro.AvroSchemaUtil.convert;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.hadoop.BloomFilterReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestParquetRowGroupResidualFilter {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(
              2, "_dict", Types.IntegerType.get()), // dict encoded, has no bloom filter
          Types.NestedField.required(
              3, "_bloom", Types.IntegerType.get()) // not dict encoded, has bloom filter
          );
  private static final int VALUE_MIN = 0;
  private static final int VALUE_MAX = 100;
  private static final int VALUE_MISSING = 10;

  private BlockMetaData rowGroup;
  private ParquetFileReader fileReader;
  private MessageType parquetSchema;

  @TempDir private Path temp;

  @BeforeEach
  public void createInputFile() throws IOException {
    File parquetFile = temp.toFile();

    assertThat(parquetFile.delete()).isTrue();

    OutputFile outFile = Files.localOutput(parquetFile);
    try (FileAppender<GenericData.Record> appender =
        Parquet.write(outFile)
            .schema(SCHEMA)
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_bloom", "true")
            // limit the dict size to prevent the `_bloom` column from forming a dictionary.
            .set(PARQUET_DICT_SIZE_BYTES, String.valueOf(20 * 4))
            .build()) {

      GenericRecordBuilder builder = new GenericRecordBuilder(convert(SCHEMA, "table"));

      for (int i = VALUE_MIN; i <= VALUE_MAX; i += 1) {
        builder.set("_dict", (i % 5 == 0) && (i != VALUE_MISSING) ? i : 0);
        builder.set("_bloom", i == VALUE_MISSING ? 0 : i);
        appender.add(builder.build());
      }
    }

    InputFile inFile = Files.localInput(parquetFile);
    fileReader = ParquetFileReader.open(ParquetIO.file(inFile));
    assertThat(fileReader.getRowGroups()).as("Should create only one row group").hasSize(1);

    parquetSchema = fileReader.getFileMetaData().getSchema();
    rowGroup = fileReader.getRowGroups().get(0);

    for (ColumnChunkMetaData column : rowGroup.getColumns()) {
      String columnPath = column.getPath().toDotString();
      if (Objects.equals(columnPath, "_dict")) {
        assertThat(ParquetUtil.hasNonDictionaryPages(column)).isFalse();
        assertThat(ParquetUtil.hasNoBloomFilterPages(column)).isTrue();
      } else if (Objects.equals(columnPath, "_bloom")) {
        assertThat(ParquetUtil.hasNonDictionaryPages(column)).isTrue();
        assertThat(ParquetUtil.hasNoBloomFilterPages(column)).isFalse();
      }
    }
  }

  @Test
  public void testMetricResidualFilter() {
    Expression expr = Expressions.greaterThan("_dict", VALUE_MAX);
    ParquetMetricsRowGroupFilter metricFilter = new ParquetMetricsRowGroupFilter(SCHEMA, expr);

    Expression expected = Expressions.alwaysFalse();
    Expression actual = metricFilter.residualFor(parquetSchema, rowGroup);

    assertThat(expected.isEquivalentTo(actual))
        .as("Expected residual: %s, actual residual: %s", expected, actual)
        .isTrue();

    expr = Expressions.equal("_dict", VALUE_MISSING);
    metricFilter = new ParquetMetricsRowGroupFilter(SCHEMA, expr);

    expected = Binder.bind(SCHEMA.asStruct(), expr, true);
    actual = metricFilter.residualFor(parquetSchema, rowGroup);

    assertThat(expected.isEquivalentTo(actual))
        .as("Expected residual: %s, actual residual: %s", expected, actual)
        .isTrue();
  }

  @Test
  public void testDictResidualFilter() {
    DictionaryPageReadStore dictionaryReader = fileReader.getDictionaryReader(rowGroup);

    Expression expr = Expressions.equal("_dict", VALUE_MISSING);
    ParquetDictionaryRowGroupFilter dictFilter = new ParquetDictionaryRowGroupFilter(SCHEMA, expr);

    Expression expected = Expressions.alwaysFalse();
    Expression actual = dictFilter.residualFor(parquetSchema, rowGroup, dictionaryReader);

    assertThat(expected.isEquivalentTo(actual))
        .as("Expected residual: %s, actual residual: %s", expected, actual)
        .isTrue();

    expr = Expressions.equal("_dict", 0);
    dictFilter = new ParquetDictionaryRowGroupFilter(SCHEMA, expr);

    expected = Binder.bind(SCHEMA.asStruct(), expr, true);
    actual = dictFilter.residualFor(parquetSchema, rowGroup, dictionaryReader);

    assertThat(expected.isEquivalentTo(actual))
        .as("Expected residual: %s, actual residual: %s", expected, actual)
        .isTrue();
  }

  @Test
  public void testBloomResidualFilter() {
    BloomFilterReader bloomFilterReader = fileReader.getBloomFilterDataReader(rowGroup);

    Expression expr = Expressions.equal("_bloom", VALUE_MISSING);
    ParquetBloomRowGroupFilter bloomFilter = new ParquetBloomRowGroupFilter(SCHEMA, expr);

    Expression expected = Expressions.alwaysFalse();
    Expression actual = bloomFilter.residualFor(parquetSchema, rowGroup, bloomFilterReader);

    assertThat(expected.isEquivalentTo(actual))
        .as("Expected residual: %s, actual residual: %s", expected, actual)
        .isTrue();

    expr = Expressions.equal("_bloom", 0);
    bloomFilter = new ParquetBloomRowGroupFilter(SCHEMA, expr);

    expected = Binder.bind(SCHEMA.asStruct(), expr, true);
    actual = bloomFilter.residualFor(parquetSchema, rowGroup, bloomFilterReader);

    assertThat(expected.isEquivalentTo(actual))
        .as("Expected residual: %s, actual residual: %s", expected, actual)
        .isTrue();
  }

  @Test
  public void testCombinedResidualFilter() {
    // will be evaluated as ROWS_MIGHT_MATCH by the metric filter and ROWS_CANNOT_MATCH by the dict
    // filter
    Expression expr1 = Expressions.equal("_dict", VALUE_MISSING);
    ParquetCombinedRowGroupFilter combinedFilter =
        new ParquetCombinedRowGroupFilter(SCHEMA, expr1, true);

    Expression expected = Expressions.alwaysFalse();
    Expression actual = combinedFilter.residualFor(parquetSchema, rowGroup, fileReader);
    assertThat(expected.isEquivalentTo(actual))
        .as("Expected residual: %s, actual residual: %s", expected, actual)
        .isTrue();

    // will be evaluated as ROWS_MIGHT_MATCH by the metric filter and ROWS_CANNOT_MATCH by the bloom
    // filter
    // dict filter cannot evaluate this since `_bloom` is not dict encoded
    Expression expr2 = Expressions.equal("_bloom", VALUE_MISSING);
    combinedFilter = new ParquetCombinedRowGroupFilter(SCHEMA, expr2, true);

    actual = combinedFilter.residualFor(parquetSchema, rowGroup, fileReader);

    assertThat(expected.isEquivalentTo(actual))
        .as("Expected residual: %s, actual residual: %s", expected, actual)
        .isTrue();

    Expression expr3 = Expressions.or(expr1, expr2);
    combinedFilter = new ParquetCombinedRowGroupFilter(SCHEMA, expr3, true);

    actual = combinedFilter.residualFor(parquetSchema, rowGroup, fileReader);

    assertThat(expected.isEquivalentTo(actual))
        .as("Expected residual: %s, actual residual: %s", expected, actual)
        .isTrue();
  }
}

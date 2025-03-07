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
package org.apache.iceberg.spark.data.parquet.vectorized;

import static org.apache.iceberg.TableProperties.PARQUET_DICT_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_PAGE_ROW_LIMIT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestParquetDictionaryEncodedVectorizedReads extends TestParquetVectorizedReads {

  protected static SparkSession spark = null;

  @BeforeAll
  public static void startSpark() {
    spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterAll
  public static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Override
  Iterable<GenericData.Record> generateData(
      Schema schema,
      int numRecords,
      long seed,
      float nullPercentage,
      Function<GenericData.Record, GenericData.Record> transform) {
    Iterable data =
        RandomData.generateDictionaryEncodableData(schema, numRecords, seed, nullPercentage);
    return transform == IDENTITY ? data : Iterables.transform(data, transform);
  }

  @Test
  @Override
  @Disabled // Ignored since this code path is already tested in TestParquetVectorizedReads
  public void testVectorizedReadsWithNewContainers() throws IOException {}

  @Test
  public void testMixedDictionaryNonDictionaryReads() throws IOException {
    Schema schema = new Schema(SUPPORTED_PRIMITIVES.fields());
    File dictionaryEncodedFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(dictionaryEncodedFile.delete()).as("Delete should succeed").isTrue();
    Iterable<GenericData.Record> dictionaryEncodableData =
        RandomData.generateDictionaryEncodableData(
            schema, 10000, 0L, RandomData.DEFAULT_NULL_PERCENTAGE);
    try (FileAppender<GenericData.Record> writer =
        getParquetWriter(schema, dictionaryEncodedFile)) {
      writer.addAll(dictionaryEncodableData);
    }

    File plainEncodingFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(plainEncodingFile.delete()).as("Delete should succeed").isTrue();
    Iterable<GenericData.Record> nonDictionaryData =
        RandomData.generate(schema, 10000, 0L, RandomData.DEFAULT_NULL_PERCENTAGE);
    try (FileAppender<GenericData.Record> writer = getParquetWriter(schema, plainEncodingFile)) {
      writer.addAll(nonDictionaryData);
    }

    int rowGroupSize = PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT;
    File mixedFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(mixedFile.delete()).as("Delete should succeed").isTrue();
    Parquet.concat(
        ImmutableList.of(dictionaryEncodedFile, plainEncodingFile, dictionaryEncodedFile),
        mixedFile,
        rowGroupSize,
        schema,
        ImmutableMap.of());
    assertRecordsMatch(
        schema,
        30000,
        FluentIterable.concat(dictionaryEncodableData, nonDictionaryData, dictionaryEncodableData),
        mixedFile,
        true,
        BATCH_SIZE);
  }

  @Test
  public void testBinaryNotAllPagesDictionaryEncoded() throws IOException {
    Schema schema = new Schema(Types.NestedField.required(1, "bytes", Types.BinaryType.get()));
    File parquetFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(parquetFile.delete()).as("Delete should succeed").isTrue();

    Iterable<GenericData.Record> records = RandomData.generateFallbackData(schema, 500, 0L, 100);
    try (FileAppender<GenericData.Record> writer =
        Parquet.write(Files.localOutput(parquetFile))
            .schema(schema)
            .set(PARQUET_DICT_SIZE_BYTES, "4096")
            .set(PARQUET_PAGE_ROW_LIMIT, "100")
            .build()) {
      writer.addAll(records);
    }

    // After the above, parquetFile contains one column chunk of binary data in five pages,
    // the first two RLE dictionary encoded, and the remaining three plain encoded.
    assertRecordsMatch(schema, 500, records, parquetFile, true, BATCH_SIZE);
  }

  /**
   * decimal_dict_and_plain_encoding.parquet contains one column chunk of decimal(38, 0) data in two
   * pages, one RLE dictionary encoded and one plain encoded, each with 200 rows.
   */
  @Test
  public void testDecimalNotAllPagesDictionaryEncoded() throws Exception {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.DecimalType.of(38, 0)));
    Path path =
        Paths.get(
            getClass()
                .getClassLoader()
                .getResource("decimal_dict_and_plain_encoding.parquet")
                .toURI());

    Dataset<Row> df = spark.read().parquet(path.toString());
    List<Row> expected = df.collectAsList();
    long expectedSize = df.count();

    Parquet.ReadBuilder readBuilder =
        Parquet.read(Files.localInput(path.toFile()))
            .project(schema)
            .createBatchedReaderFunc(
                type ->
                    VectorizedSparkParquetReaders.buildReader(
                        schema, type, ImmutableMap.of(), null));

    try (CloseableIterable<ColumnarBatch> batchReader = readBuilder.build()) {
      Iterator<Row> expectedIter = expected.iterator();
      Iterator<ColumnarBatch> batches = batchReader.iterator();
      int numRowsRead = 0;
      while (batches.hasNext()) {
        ColumnarBatch batch = batches.next();
        numRowsRead += batch.numRows();
        TestHelpers.assertEqualsBatchWithRows(schema.asStruct(), expectedIter, batch);
      }
      assertThat(numRowsRead).isEqualTo(expectedSize);
    }
  }
}

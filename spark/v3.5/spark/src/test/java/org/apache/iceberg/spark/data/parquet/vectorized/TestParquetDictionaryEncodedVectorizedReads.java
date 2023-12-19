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

import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.data.RandomData;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestParquetDictionaryEncodedVectorizedReads extends TestParquetVectorizedReads {

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
    File dictionaryEncodedFile = temp.toFile();
    assertThat(dictionaryEncodedFile.delete()).as("Delete should succeed").isTrue();
    Iterable<GenericData.Record> dictionaryEncodableData =
        RandomData.generateDictionaryEncodableData(
            schema, 10000, 0L, RandomData.DEFAULT_NULL_PERCENTAGE);
    try (FileAppender<GenericData.Record> writer =
        getParquetWriter(schema, dictionaryEncodedFile)) {
      writer.addAll(dictionaryEncodableData);
    }

    File plainEncodingFile = temp.toFile();
    assertThat(plainEncodingFile.delete()).as("Delete should succeed").isTrue();
    Iterable<GenericData.Record> nonDictionaryData =
        RandomData.generate(schema, 10000, 0L, RandomData.DEFAULT_NULL_PERCENTAGE);
    try (FileAppender<GenericData.Record> writer = getParquetWriter(schema, plainEncodingFile)) {
      writer.addAll(nonDictionaryData);
    }

    int rowGroupSize = PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT;
    File mixedFile = temp.toFile();
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
}

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

import java.io.File;
import java.io.IOException;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.data.RandomData;
import org.junit.Ignore;
import org.junit.Test;

public class TestParquetDictionaryFallbackToPlainEncodingVectorizedReads
    extends TestParquetVectorizedReads {
  private static final int NUM_ROWS = 1_000_000;

  @Override
  protected int getNumRows() {
    return NUM_ROWS;
  }

  @Override
  Iterable<GenericData.Record> generateData(
      Schema schema,
      int numRecords,
      long seed,
      float nullPercentage,
      Function<GenericData.Record, GenericData.Record> transform) {
    // TODO: take into account nullPercentage when generating fallback encoding data
    Iterable data = RandomData.generateFallbackData(schema, numRecords, seed, numRecords / 20);
    return transform == IDENTITY ? data : Iterables.transform(data, transform);
  }

  @Override
  FileAppender<GenericData.Record> getParquetWriter(Schema schema, File testFile)
      throws IOException {
    return Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .named("test")
        .set(TableProperties.PARQUET_DICT_SIZE_BYTES, "512000")
        .build();
  }

  @Test
  @Override
  @Ignore // Fallback encoding not triggered when data is mostly null
  public void testMostlyNullsForOptionalFields() {}

  @Test
  @Override
  @Ignore // Ignored since this code path is already tested in TestParquetVectorizedReads
  public void testVectorizedReadsWithNewContainers() throws IOException {}
}

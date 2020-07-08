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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.spark.data.TestHelpers.assertEqualsUnsafe;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestSparkParquetReader extends AvroDataTest {
  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    Assume.assumeTrue("Parquet Avro cannot write non-string map keys", null == TypeUtil.find(schema,
        type -> type.isMapType() && type.asMapType().keyType() != Types.StringType.get()));

    List<GenericData.Record> expected = RandomData.generateList(schema, 100, 0L);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<GenericData.Record> writer = Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .named("test")
        .build()) {
      writer.addAll(expected);
    }

    try (CloseableIterable<InternalRow> reader = Parquet.read(Files.localInput(testFile))
        .project(schema)
        .createReaderFunc(type -> SparkParquetReaders.buildReader(schema, type))
        .build()) {
      Iterator<InternalRow> rows = reader.iterator();
      for (int i = 0; i < expected.size(); i += 1) {
        Assert.assertTrue("Should have expected number of rows", rows.hasNext());
        assertEqualsUnsafe(schema.asStruct(), expected.get(i), rows.next());
      }
      Assert.assertFalse("Should not have extra rows", rows.hasNext());
    }
  }

  protected List<InternalRow> rowsFromFile(InputFile inputFile, Schema schema) throws IOException {
    try (CloseableIterable<InternalRow> reader =
        Parquet.read(inputFile)
            .project(schema)
            .createReaderFunc(type -> SparkParquetReaders.buildReader(schema, type))
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  @Test
  public void testInt96TimestampProducedBySparkIsReadCorrectly() throws IOException {
    final SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.sql.parquet.int96AsTimestamp", "false")
            .getOrCreate();

    final String parquetPath = temp.getRoot().getAbsolutePath() + "/parquet_int96";
    final java.sql.Timestamp ts = java.sql.Timestamp.valueOf("2014-01-01 23:00:01");
    spark.createDataset(ImmutableList.of(ts), Encoders.TIMESTAMP()).write().parquet(parquetPath);
    spark.stop();

    // Get the single parquet file produced by spark.
    List<Path> parquetOutputs =
        java.nio.file.Files.find(
            java.nio.file.Paths.get(parquetPath),
            1,
            (path, basicFileAttributes) -> path.toString().endsWith(".parquet"))
        .collect(Collectors.toList());
    Assert.assertEquals(1, parquetOutputs.size());

    List<InternalRow> rows =
        rowsFromFile(
            Files.localInput(parquetOutputs.get(0).toFile()),
            new Schema(optional(1, "timestamp", Types.TimestampType.withoutZone())));
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(1, rows.get(0).numFields());

    // Spark represents Timestamps as epoch micros and are stored as longs.
    Assert.assertEquals(
        ts.toInstant(),
        Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(rows.get(0).getLong(0))));
  }
}

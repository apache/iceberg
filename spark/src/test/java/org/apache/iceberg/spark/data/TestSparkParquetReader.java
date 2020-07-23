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
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.spark.data.TestHelpers.assertEqualsUnsafe;
import static org.apache.iceberg.types.Types.NestedField.required;

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
    final Schema schema = new Schema(required(1, "ts", Types.TimestampType.asSparkInt96()));
    final StructType sparkSchema = SparkSchemaUtil.convert(schema);
    final Path parquetFile = Paths.get(temp.getRoot().getAbsolutePath(), "parquet_int96.parquet");
    final List<InternalRow> rows = Lists.newArrayList(RandomData.generateSpark(schema, 10, 0L));

    try (FileAppender<InternalRow> writer =
        Parquet.write(Files.localOutput(parquetFile.toString()))
            .writeSupport(
                new org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport())
            .set("org.apache.spark.sql.parquet.row.attributes", sparkSchema.json())
            .set("org.apache.spark.legacyDateTime", "false")
            .set("spark.sql.parquet.int96AsTimestamp", "true")
            .set("spark.sql.parquet.writeLegacyFormat", "false")
            .set("spark.sql.parquet.outputTimestampType", "INT96")
            .schema(schema)
            .build()) {
      writer.addAll(rows);
    }

    final List<InternalRow> readRows =
        rowsFromFile(Files.localInput(parquetFile.toString()), schema);
    Assert.assertEquals(rows.size(), readRows.size());
    Assert.assertThat(readRows, CoreMatchers.is(rows));
  }
}

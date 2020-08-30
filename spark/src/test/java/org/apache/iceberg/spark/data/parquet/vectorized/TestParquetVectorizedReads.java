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
import java.util.Iterator;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.data.AvroDataTest;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestParquetVectorizedReads extends AvroDataTest {
  private static final int NUM_ROWS = 200_000;

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(schema, getNumRows(), 0L, RandomData.DEFAULT_NULL_PERCENTAGE, false, true);
  }

  private void writeAndValidate(
      Schema schema, int numRecords, long seed, float nullPercentage,
      boolean setAndCheckArrowValidityVector, boolean reuseContainers)
      throws IOException {
    // Write test data
    Assume.assumeTrue("Parquet Avro cannot write non-string map keys", null == TypeUtil.find(
        schema,
        type -> type.isMapType() && type.asMapType().keyType() != Types.StringType.get()));

    Iterable<GenericData.Record> expected = generateData(schema, numRecords, seed, nullPercentage);

    // write a test parquet file using iceberg writer
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<GenericData.Record> writer = getParquetWriter(schema, testFile)) {
      writer.addAll(expected);
    }
    assertRecordsMatch(schema, numRecords, expected, testFile, setAndCheckArrowValidityVector, reuseContainers);
  }

  protected int getNumRows() {
    return NUM_ROWS;
  }

  Iterable<GenericData.Record> generateData(Schema schema, int numRecords, long seed, float nullPercentage) {
    return RandomData.generate(schema, numRecords, seed, nullPercentage);
  }

  FileAppender<GenericData.Record> getParquetWriter(Schema schema, File testFile) throws IOException {
    return Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .named("test")
        .build();
  }

  void assertRecordsMatch(
      Schema schema, int expectedSize, Iterable<GenericData.Record> expected, File testFile,
      boolean setAndCheckArrowValidityBuffer, boolean reuseContainers)
      throws IOException {
    Parquet.ReadBuilder readBuilder = Parquet.read(Files.localInput(testFile))
        .project(schema)
        .recordsPerBatch(10000)
        .createBatchedReaderFunc(type -> VectorizedSparkParquetReaders.buildReader(
            schema,
            type,
            setAndCheckArrowValidityBuffer));
    if (reuseContainers) {
      readBuilder.reuseContainers();
    }
    try (CloseableIterable<ColumnarBatch> batchReader =
        readBuilder.build()) {
      Iterator<GenericData.Record> expectedIter = expected.iterator();
      Iterator<ColumnarBatch> batches = batchReader.iterator();
      int numRowsRead = 0;
      while (batches.hasNext()) {
        ColumnarBatch batch = batches.next();
        numRowsRead += batch.numRows();
        TestHelpers.assertEqualsBatch(schema.asStruct(), expectedIter, batch, setAndCheckArrowValidityBuffer);
      }
      Assert.assertEquals(expectedSize, numRowsRead);
    }
  }

  @Test
  @Ignore
  public void testArray() {
  }

  @Test
  @Ignore
  public void testArrayOfStructs() {
  }

  @Test
  @Ignore
  public void testMap() {
  }

  @Test
  @Ignore
  public void testNumericMapKey() {
  }

  @Test
  @Ignore
  public void testComplexMapKey() {
  }

  @Test
  @Ignore
  public void testMapOfStructs() {
  }

  @Test
  @Ignore
  public void testMixedTypes() {
  }

  @Test
  @Override
  public void testNestedStruct() {
    AssertHelpers.assertThrows(
        "Vectorized reads are not supported yet for struct fields",
        UnsupportedOperationException.class,
        "Vectorized reads are not supported yet for struct fields",
        () -> VectorizedSparkParquetReaders.buildReader(
            TypeUtil.assignIncreasingFreshIds(new Schema(required(
                1,
                "struct",
                SUPPORTED_PRIMITIVES))),
            new MessageType("struct", new GroupType(Type.Repetition.OPTIONAL, "struct").withId(1)),
            false));
  }

  @Test
  public void testMostlyNullsForOptionalFields() throws IOException {
    writeAndValidate(
        TypeUtil.assignIncreasingFreshIds(new Schema(SUPPORTED_PRIMITIVES.fields())),
        getNumRows(),
        0L,
        0.99f,
        false,
        true);
  }

  @Test
  public void testSettingArrowValidityVector() throws IOException {
    writeAndValidate(new Schema(
            Lists.transform(SUPPORTED_PRIMITIVES.fields(), Types.NestedField::asOptional)),
        getNumRows(), 0L, RandomData.DEFAULT_NULL_PERCENTAGE, true, true);
  }

  @Test
  public void testVectorizedReadsWithNewContainers() throws IOException {
    writeAndValidate(TypeUtil.assignIncreasingFreshIds(new Schema(SUPPORTED_PRIMITIVES.fields())),
        getNumRows(), 0L, RandomData.DEFAULT_NULL_PERCENTAGE, true, false);
  }
}

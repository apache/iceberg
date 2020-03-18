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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestSparkParquetVectorizedReader extends AvroDataTest {

  @BeforeClass
  public static void beforeClass() {
    TestHelpers.setArrowFlagsForVectorizedReads();
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    // Write test data
    Assume.assumeTrue("Parquet Avro cannot write non-string map keys", null == TypeUtil.find(
        schema,
        type -> type.isMapType() && type.asMapType().keyType() != Types.StringType.get()));

    List<GenericData.Record> expected = generateData(schema);

    // write a test parquet file using iceberg writer
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<GenericData.Record> writer = Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .named("test")
        .build()) {
      writer.addAll(expected);
    }
    assertRecordsMatch(schema, expected, testFile);
  }

  List<GenericData.Record> generateData(Schema schema) {
    return RandomData.generateList(schema, 100000, 0L);
  }

  void assertRecordsMatch(Schema schema, List<GenericData.Record> expected, File testFile) throws IOException {
    try (CloseableIterable<ColumnarBatch> batchReader = Parquet.read(Files.localInput(testFile))
        .project(schema)
        .reuseContainers()
        .createBatchedReaderFunc(type -> VectorizedSparkParquetReaders.buildReader(schema, schema, type, 10000))
        .build()) {

      Iterator<ColumnarBatch> batches = batchReader.iterator();
      int numRowsRead = 0;
      int numExpectedRead = 0;
      while (batches.hasNext()) {
        ColumnarBatch batch = batches.next();
        numRowsRead += batch.numRows();
        List<GenericData.Record> expectedBatch = new ArrayList<>(batch.numRows());
        for (int i = numExpectedRead; i < numExpectedRead + batch.numRows(); i++) {
          expectedBatch.add(expected.get(i));
        }
        TestHelpers.assertArrowVectors(schema.asStruct(), expectedBatch, batch);
        numExpectedRead += batch.numRows();
      }
      Assert.assertEquals(expected.size(), numRowsRead);
    }
  }

  @Test
  @Ignore
  public void testArray() {
  }

  @Test
  @Ignore
  public void testArrayOfStructs() {
    System.out.println("Not Supported");
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
}

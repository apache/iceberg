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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Iterator;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.data.AvroDataTest;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

public class TestParquetVectorizedReads extends AvroDataTest {
  private static final int NUM_ROWS = 200_000;
  private static final ByteBuffer fileDek = ByteBuffer.allocate(16);
  private static final ByteBuffer aadPrefix = ByteBuffer.allocate(16);

  static final int BATCH_SIZE = 10_000;
  static final Function<GenericData.Record, GenericData.Record> IDENTITY = record -> record;

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(schema, getNumRows(), 0L, RandomData.DEFAULT_NULL_PERCENTAGE, true);
  }

  private void writeAndValidate(
      Schema schema, int numRecords, long seed, float nullPercentage, boolean reuseContainers)
      throws IOException {
    writeAndValidate(
        schema, numRecords, seed, nullPercentage, reuseContainers, BATCH_SIZE, IDENTITY);
  }

  private void writeAndValidate(
      Schema schema,
      int numRecords,
      long seed,
      float nullPercentage,
      boolean reuseContainers,
      int batchSize,
      Function<GenericData.Record, GenericData.Record> transform)
      throws IOException {
    // Write test data
    Assume.assumeTrue(
        "Parquet Avro cannot write non-string map keys",
        null
            == TypeUtil.find(
                schema,
                type -> type.isMapType() && type.asMapType().keyType() != Types.StringType.get()));

    Iterable<GenericData.Record> expected =
        generateData(schema, numRecords, seed, nullPercentage, transform);

    // write a test parquet file using iceberg writer
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<GenericData.Record> writer = parquetWriter(schema, testFile)) {
      writer.addAll(expected);
    }
    assertRecordsMatch(schema, numRecords, expected, testFile, reuseContainers, batchSize);

    // With encryption
    testFile.delete();
    try (FileAppender<GenericData.Record> writer = encryptedParquetWriter(schema, testFile)) {
      writer.addAll(expected);
    }

    assertRecordsMatch(schema, numRecords, expected, testFile, reuseContainers, batchSize, true);
  }

  protected int getNumRows() {
    return NUM_ROWS;
  }

  Iterable<GenericData.Record> generateData(
      Schema schema,
      int numRecords,
      long seed,
      float nullPercentage,
      Function<GenericData.Record, GenericData.Record> transform) {
    Iterable<GenericData.Record> data =
        RandomData.generate(schema, numRecords, seed, nullPercentage);
    return transform == IDENTITY ? data : Iterables.transform(data, transform);
  }

  FileAppender<GenericData.Record> parquetWriter(Schema schema, File testFile) throws IOException {
    return Parquet.write(Files.localOutput(testFile)).schema(schema).named("test").build();
  }

  FileAppender<GenericData.Record> encryptedParquetWriter(Schema schema, File testFile)
      throws IOException {
    SecureRandom rand = new SecureRandom();
    rand.nextBytes(fileDek.array());
    rand.nextBytes(aadPrefix.array());
    return Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .withFileEncryptionKey(fileDek)
        .withAADPrefix(aadPrefix)
        .named("test")
        .build();
  }

  FileAppender<GenericData.Record> parquetV2Writer(Schema schema, File testFile)
      throws IOException {
    return Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .named("test")
        .writerVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
        .build();
  }

  FileAppender<GenericData.Record> encryptedParquetV2Writer(Schema schema, File testFile)
      throws IOException {
    SecureRandom rand = new SecureRandom();
    rand.nextBytes(fileDek.array());
    rand.nextBytes(aadPrefix.array());
    return Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .withFileEncryptionKey(fileDek)
        .withAADPrefix(aadPrefix)
        .named("test")
        .writerVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
        .build();
  }

  void assertRecordsMatch(
      Schema schema,
      int expectedSize,
      Iterable<GenericData.Record> expected,
      File testFile,
      boolean reuseContainers,
      int batchSize)
      throws IOException {
    assertRecordsMatch(schema, expectedSize, expected, testFile, reuseContainers, batchSize, false);
  }

  void assertRecordsMatch(
      Schema schema,
      int expectedSize,
      Iterable<GenericData.Record> expected,
      File testFile,
      boolean reuseContainers,
      int batchSize,
      boolean encrypted)
      throws IOException {
    Parquet.ReadBuilder readBuilder =
        Parquet.read(Files.localInput(testFile))
            .project(schema)
            .recordsPerBatch(batchSize)
            .createBatchedReaderFunc(
                type ->
                    VectorizedSparkParquetReaders.buildReader(
                        schema, type, Maps.newHashMap(), null));
    if (reuseContainers) {
      readBuilder.reuseContainers();
    }

    if (encrypted) {
      readBuilder.withFileEncryptionKey(fileDek);
      readBuilder.withAADPrefix(aadPrefix);
    }

    try (CloseableIterable<ColumnarBatch> batchReader = readBuilder.build()) {
      Iterator<GenericData.Record> expectedIter = expected.iterator();
      Iterator<ColumnarBatch> batches = batchReader.iterator();
      int numRowsRead = 0;
      while (batches.hasNext()) {
        ColumnarBatch batch = batches.next();
        numRowsRead += batch.numRows();
        TestHelpers.assertEqualsBatch(schema.asStruct(), expectedIter, batch);
      }
      Assert.assertEquals(expectedSize, numRowsRead);
    }
  }

  @Override
  @Test
  @Ignore
  public void testArray() {}

  @Override
  @Test
  @Ignore
  public void testArrayOfStructs() {}

  @Override
  @Test
  @Ignore
  public void testMap() {}

  @Override
  @Test
  @Ignore
  public void testNumericMapKey() {}

  @Override
  @Test
  @Ignore
  public void testComplexMapKey() {}

  @Override
  @Test
  @Ignore
  public void testMapOfStructs() {}

  @Override
  @Test
  @Ignore
  public void testMixedTypes() {}

  @Test
  @Override
  public void testNestedStruct() {
    assertThatThrownBy(
            () ->
                VectorizedSparkParquetReaders.buildReader(
                    TypeUtil.assignIncreasingFreshIds(
                        new Schema(required(1, "struct", SUPPORTED_PRIMITIVES))),
                    new MessageType(
                        "struct", new GroupType(Type.Repetition.OPTIONAL, "struct").withId(1)),
                    Maps.newHashMap(),
                    null))
        .as("Vectorized reads are not supported yet for struct fields")
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Vectorized reads are not supported yet for struct fields");
  }

  @Test
  public void testMostlyNullsForOptionalFields() throws IOException {
    writeAndValidate(
        TypeUtil.assignIncreasingFreshIds(new Schema(SUPPORTED_PRIMITIVES.fields())),
        getNumRows(),
        0L,
        0.99f,
        true);
  }

  @Test
  public void testSettingArrowValidityVector() throws IOException {
    writeAndValidate(
        new Schema(Lists.transform(SUPPORTED_PRIMITIVES.fields(), Types.NestedField::asOptional)),
        getNumRows(),
        0L,
        RandomData.DEFAULT_NULL_PERCENTAGE,
        true);
  }

  @Test
  public void testVectorizedReadsWithNewContainers() throws IOException {
    writeAndValidate(
        TypeUtil.assignIncreasingFreshIds(new Schema(SUPPORTED_PRIMITIVES.fields())),
        getNumRows(),
        0L,
        RandomData.DEFAULT_NULL_PERCENTAGE,
        false);
  }

  @Test
  public void testVectorizedReadsWithReallocatedArrowBuffers() throws IOException {
    // With a batch size of 2, 256 bytes are allocated in the VarCharVector. By adding strings of
    // length 512, the vector will need to be reallocated for storing the batch.
    writeAndValidate(
        new Schema(
            Lists.newArrayList(
                SUPPORTED_PRIMITIVES.field("id"), SUPPORTED_PRIMITIVES.field("data"))),
        10,
        0L,
        RandomData.DEFAULT_NULL_PERCENTAGE,
        true,
        2,
        record -> {
          if (record.get("data") != null) {
            record.put("data", Strings.padEnd((String) record.get("data"), 512, 'a'));
          } else {
            record.put("data", Strings.padEnd("", 512, 'a'));
          }
          return record;
        });
  }

  @Test
  public void testReadsForTypePromotedColumns() throws Exception {
    Schema writeSchema =
        new Schema(
            required(100, "id", Types.LongType.get()),
            optional(101, "int_data", Types.IntegerType.get()),
            optional(102, "float_data", Types.FloatType.get()),
            optional(103, "decimal_data", Types.DecimalType.of(10, 5)));

    File dataFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", dataFile.delete());
    Iterable<GenericData.Record> data =
        generateData(writeSchema, 30000, 0L, RandomData.DEFAULT_NULL_PERCENTAGE, IDENTITY);
    try (FileAppender<GenericData.Record> writer = parquetWriter(writeSchema, dataFile)) {
      writer.addAll(data);
    }

    Schema readSchema =
        new Schema(
            required(100, "id", Types.LongType.get()),
            optional(101, "int_data", Types.LongType.get()),
            optional(102, "float_data", Types.DoubleType.get()),
            optional(103, "decimal_data", Types.DecimalType.of(25, 5)));

    assertRecordsMatch(readSchema, 30000, data, dataFile, true, BATCH_SIZE);
  }

  @Test
  public void testSupportedReadsForParquetV2() throws Exception {
    // Float and double column types are written using plain encoding with Parquet V2,
    // also Parquet V2 will dictionary encode decimals that use fixed length binary
    // (i.e. decimals > 8 bytes)
    Schema schema =
        new Schema(
            optional(102, "float_data", Types.FloatType.get()),
            optional(103, "double_data", Types.DoubleType.get()),
            optional(104, "decimal_data", Types.DecimalType.of(25, 5)));

    File dataFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", dataFile.delete());
    Iterable<GenericData.Record> data =
        generateData(schema, 30000, 0L, RandomData.DEFAULT_NULL_PERCENTAGE, IDENTITY);
    try (FileAppender<GenericData.Record> writer = parquetV2Writer(schema, dataFile)) {
      writer.addAll(data);
    }
    assertRecordsMatch(schema, 30000, data, dataFile, true, BATCH_SIZE);

    // With encryption
    dataFile.delete();
    try (FileAppender<GenericData.Record> writer = encryptedParquetV2Writer(schema, dataFile)) {
      writer.addAll(data);
    }

    assertRecordsMatch(schema, 30000, data, dataFile, true, BATCH_SIZE, true);
  }

  @Test
  public void testUnsupportedReadsForParquetV2() throws Exception {
    // Longs, ints, string types etc use delta encoding and which are not supported for vectorized
    // reads
    Schema schema = new Schema(SUPPORTED_PRIMITIVES.fields());
    File dataFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", dataFile.delete());
    Iterable<GenericData.Record> data =
        generateData(schema, 30000, 0L, RandomData.DEFAULT_NULL_PERCENTAGE, IDENTITY);
    try (FileAppender<GenericData.Record> writer = parquetV2Writer(schema, dataFile)) {
      writer.addAll(data);
    }
    assertThatThrownBy(() -> assertRecordsMatch(schema, 30000, data, dataFile, true, BATCH_SIZE))
        .as("Vectorized reads not supported")
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Cannot support vectorized reads for column");
  }
}

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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Consumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.arrow.ArrowAllocation;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.data.AvroDataTestBase;
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
import org.junit.jupiter.api.Test;

public class TestParquetVectorizedReads extends AvroDataTestBase {
  private static final int NUM_ROWS = 200_000;
  static final int BATCH_SIZE = 10_000;

  static final Function<GenericData.Record, GenericData.Record> IDENTITY = record -> record;

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(schema, schema);
  }

  @Override
  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema) throws IOException {
    writeAndValidate(
        writeSchema,
        expectedSchema,
        getNumRows(),
        29714278L,
        RandomData.DEFAULT_NULL_PERCENTAGE,
        true,
        BATCH_SIZE,
        IDENTITY);
  }

  @Override
  protected boolean supportsDefaultValues() {
    return true;
  }

  @Override
  protected boolean supportsNestedTypes() {
    return false;
  }

  private void writeAndValidate(
      Schema schema, int numRecords, long seed, float nullPercentage, boolean reuseContainers)
      throws IOException {
    writeAndValidate(
        schema, schema, numRecords, seed, nullPercentage, reuseContainers, BATCH_SIZE, IDENTITY);
  }

  private void writeAndValidate(
      Schema writeSchema,
      Schema expectedSchema,
      int numRecords,
      long seed,
      float nullPercentage,
      boolean reuseContainers,
      int batchSize,
      Function<GenericData.Record, GenericData.Record> transform)
      throws IOException {
    // Write test data
    assumeThat(
            TypeUtil.find(
                writeSchema,
                type -> type.isMapType() && type.asMapType().keyType() != Types.StringType.get()))
        .as("Parquet Avro cannot write non-string map keys")
        .isNull();

    Iterable<GenericData.Record> expected =
        generateData(writeSchema, numRecords, seed, nullPercentage, transform);

    OutputFile outputFile = new InMemoryOutputFile();

    try (FileAppender<GenericData.Record> writer = getParquetWriter(writeSchema, outputFile)) {
      writer.addAll(expected);
    }

    assertRecordsMatch(
        expectedSchema, numRecords, expected, outputFile.toInputFile(), reuseContainers, batchSize);
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

  FileAppender<GenericData.Record> getParquetWriter(Schema schema, OutputFile outputFile)
      throws IOException {
    return Parquet.write(outputFile).schema(schema).named("test").build();
  }

  FileAppender<GenericData.Record> getParquetV2Writer(Schema schema, OutputFile outputFile)
      throws IOException {
    return Parquet.write(outputFile)
        .schema(schema)
        .named("test")
        .writerVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
        .build();
  }

  void assertRecordsMatch(
      Schema schema,
      int expectedSize,
      Iterable<GenericData.Record> expected,
      InputFile inputFile,
      boolean reuseContainers,
      int batchSize)
      throws IOException {
    assertNoLeak(
        inputFile.location(),
        allocator -> {
          Parquet.ReadBuilder readBuilder =
              Parquet.read(inputFile)
                  .project(schema)
                  .recordsPerBatch(batchSize)
                  .createBatchedReaderFunc(
                      type ->
                          VectorizedSparkParquetReaders.buildReader(
                              schema, type, Maps.newHashMap(), allocator));
          if (reuseContainers) {
            readBuilder.reuseContainers();
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
            assertThat(numRowsRead).isEqualTo(expectedSize);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

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
                    Maps.newHashMap()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Vectorized reads are not supported yet for struct fields");
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
    Schema schema =
        new Schema(
            Lists.newArrayList(
                SUPPORTED_PRIMITIVES.field("id"), SUPPORTED_PRIMITIVES.field("data")));
    writeAndValidate(
        schema,
        schema,
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

    OutputFile outputFile = new InMemoryOutputFile();
    Iterable<GenericData.Record> data =
        generateData(writeSchema, 30000, 0L, RandomData.DEFAULT_NULL_PERCENTAGE, IDENTITY);
    try (FileAppender<GenericData.Record> writer = getParquetWriter(writeSchema, outputFile)) {
      writer.addAll(data);
    }

    Schema readSchema =
        new Schema(
            required(100, "id", Types.LongType.get()),
            optional(101, "int_data", Types.LongType.get()),
            optional(102, "float_data", Types.DoubleType.get()),
            optional(103, "decimal_data", Types.DecimalType.of(25, 5)));

    assertRecordsMatch(readSchema, 30000, data, outputFile.toInputFile(), false, BATCH_SIZE);
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

    OutputFile outputFile = new InMemoryOutputFile();
    Iterable<GenericData.Record> data =
        generateData(schema, 30000, 0L, RandomData.DEFAULT_NULL_PERCENTAGE, IDENTITY);
    try (FileAppender<GenericData.Record> writer = getParquetV2Writer(schema, outputFile)) {
      writer.addAll(data);
    }
    assertRecordsMatch(schema, 30000, data, outputFile.toInputFile(), false, BATCH_SIZE);
  }

  @Test
  public void testUnsupportedReadsForParquetV2() throws Exception {
    // Longs, ints, string types etc use delta encoding and which are not supported for vectorized
    // reads
    Schema schema = new Schema(SUPPORTED_PRIMITIVES.fields());
    OutputFile outputFile = new InMemoryOutputFile();
    Iterable<GenericData.Record> data =
        generateData(schema, 30000, 0L, RandomData.DEFAULT_NULL_PERCENTAGE, IDENTITY);
    try (FileAppender<GenericData.Record> writer = getParquetV2Writer(schema, outputFile)) {
      writer.addAll(data);
    }
    assertThatThrownBy(
            () ->
                assertRecordsMatch(
                    schema, 30000, data, outputFile.toInputFile(), false, BATCH_SIZE))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageStartingWith("Cannot support vectorized reads for column")
        .hasMessageEndingWith("Disable vectorized reads to read this table/file");
  }

  @Test
  public void testUuidReads() throws Exception {
    // Just one row to maintain dictionary encoding
    int numRows = 1;
    Schema schema = new Schema(optional(100, "uuid", Types.UUIDType.get()));

    InMemoryOutputFile dataFile = new InMemoryOutputFile();
    Iterable<GenericData.Record> data = generateData(schema, numRows, 0L, 0, IDENTITY);
    try (FileAppender<GenericData.Record> writer = getParquetV2Writer(schema, dataFile)) {
      writer.addAll(data);
    }
    assertRecordsMatch(schema, numRows, data, dataFile.toInputFile(), false, BATCH_SIZE);
  }

  protected void assertNoLeak(String testName, Consumer<BufferAllocator> testFunction) {
    BufferAllocator allocator =
        ArrowAllocation.rootAllocator().newChildAllocator(testName, 0, Long.MAX_VALUE);
    try {
      testFunction.accept(allocator);
      assertThat(allocator.getAllocatedMemory())
          .as(
              "Should have released all memory prior to closing. Expected to find 0 bytes of memory in use.")
          .isEqualTo(0L);
    } finally {
      allocator.close();
    }
  }
}

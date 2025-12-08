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
package org.apache.iceberg.spark.data.vectorized.parquet;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.arrow.ArrowAllocation;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.data.AvroDataTestBase;
import org.apache.iceberg.spark.data.GenericsHelpers;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestParquetVectorizedReads extends AvroDataTestBase {
  private static final int NUM_ROWS = 200_000;
  static final int BATCH_SIZE = 10_000;

  private static final String PLAIN = "PLAIN";
  private static final List<String> GOLDEN_FILE_ENCODINGS =
      ImmutableList.of("PLAIN_DICTIONARY", "RLE_DICTIONARY", "DELTA_BINARY_PACKED");
  private static final Map<String, PrimitiveType> GOLDEN_FILE_TYPES =
      ImmutableMap.of(
          "string", Types.StringType.get(),
          "float", Types.FloatType.get(),
          "int32", Types.IntegerType.get(),
          "int64", Types.LongType.get(),
          "binary", Types.BinaryType.get(),
          "boolean", Types.BooleanType.get());

  static final Function<Record, Record> IDENTITY = record -> record;

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
  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema, List<Record> records)
      throws IOException {
    writeAndValidate(
        writeSchema, expectedSchema, true, BATCH_SIZE, records.size(), records, ID_TO_CONSTANT);
  }

  @Override
  protected boolean supportsDefaultValues() {
    return true;
  }

  @Override
  protected boolean supportsNestedTypes() {
    return false;
  }

  @Override
  protected boolean supportsRowLineage() {
    return true;
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
      Function<Record, Record> transform)
      throws IOException {
    writeAndValidate(
        writeSchema,
        expectedSchema,
        numRecords,
        seed,
        nullPercentage,
        reuseContainers,
        batchSize,
        transform,
        ImmutableMap.of());
  }

  private void writeAndValidate(
      Schema writeSchema,
      Schema expectedSchema,
      int numRecords,
      long seed,
      float nullPercentage,
      boolean reuseContainers,
      int batchSize,
      Function<Record, Record> transform,
      Map<Integer, Object> idToConstant)
      throws IOException {
    // Write test data
    assumeThat(
            TypeUtil.find(
                writeSchema,
                type -> type.isMapType() && type.asMapType().keyType() != Types.StringType.get()))
        .as("Parquet Avro cannot write non-string map keys")
        .isNull();

    Iterable<Record> expected =
        generateData(writeSchema, numRecords, seed, nullPercentage, transform);

    writeAndValidate(
        writeSchema,
        expectedSchema,
        reuseContainers,
        batchSize,
        numRecords,
        expected,
        idToConstant);
  }

  private void writeAndValidate(
      Schema writeSchema,
      Schema expectedSchema,
      boolean reuseContainers,
      int batchSize,
      int numRecords,
      Iterable<Record> expected,
      Map<Integer, Object> idToConstant)
      throws IOException {
    // write a test parquet file using iceberg writer
    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();

    try (FileAppender<Record> writer = getParquetWriter(writeSchema, testFile)) {
      writer.addAll(expected);
    }

    assertRecordsMatch(
        expectedSchema, numRecords, expected, testFile, reuseContainers, batchSize, idToConstant);
  }

  protected int getNumRows() {
    return NUM_ROWS;
  }

  Iterable<Record> generateData(
      Schema schema,
      int numRecords,
      long seed,
      float nullPercentage,
      Function<Record, Record> transform) {
    Iterable<Record> data = RandomGenericData.generate(schema, numRecords, seed);
    return transform == IDENTITY ? data : Iterables.transform(data, transform);
  }

  FileAppender<Record> getParquetWriter(Schema schema, File testFile) throws IOException {
    return Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::create)
        .named("test")
        .build();
  }

  FileAppender<Record> getParquetV2Writer(Schema schema, File testFile) throws IOException {
    return Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::create)
        .named("test")
        .writerVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
        .build();
  }

  void assertRecordsMatch(
      Schema schema,
      int expectedSize,
      Iterable<Record> expected,
      File testFile,
      boolean reuseContainers,
      int batchSize)
      throws IOException {
    assertRecordsMatch(
        schema, expectedSize, expected, testFile, reuseContainers, batchSize, ImmutableMap.of());
  }

  void assertRecordsMatch(
      Schema schema,
      int expectedSize,
      Iterable<Record> expected,
      File testFile,
      boolean reuseContainers,
      int batchSize,
      Map<Integer, Object> idToConstant)
      throws IOException {
    assertNoLeak(
        testFile.getName(),
        allocator -> {
          Parquet.ReadBuilder readBuilder =
              Parquet.read(Files.localInput(testFile))
                  .project(schema)
                  .recordsPerBatch(batchSize)
                  .createBatchedReaderFunc(
                      type ->
                          VectorizedSparkParquetReaders.buildReader(
                              schema, type, idToConstant, allocator));
          if (reuseContainers) {
            readBuilder.reuseContainers();
          }
          try (CloseableIterable<ColumnarBatch> batchReader = readBuilder.build()) {
            Iterator<Record> expectedIter = expected.iterator();
            Iterator<ColumnarBatch> batches = batchReader.iterator();
            int numRowsRead = 0;
            while (batches.hasNext()) {
              ColumnarBatch batch = batches.next();
              GenericsHelpers.assertEqualsBatch(
                  schema.asStruct(), expectedIter, batch, idToConstant, numRowsRead);
              numRowsRead += batch.numRows();
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
    int dataOrdinal = 1;

    writeAndValidate(
        schema,
        schema,
        10,
        0L,
        RandomData.DEFAULT_NULL_PERCENTAGE,
        true,
        2,
        record -> {
          if (record.get(dataOrdinal, String.class) != null) {
            record.set(
                dataOrdinal, Strings.padEnd(record.get(dataOrdinal, String.class), 512, 'a'));
          } else {
            record.set(dataOrdinal, Strings.padEnd("", 512, 'a'));
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

    File dataFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(dataFile.delete()).as("Delete should succeed").isTrue();
    Iterable<Record> data =
        generateData(writeSchema, 30000, 0L, RandomData.DEFAULT_NULL_PERCENTAGE, IDENTITY);
    try (FileAppender<Record> writer = getParquetWriter(writeSchema, dataFile)) {
      writer.addAll(data);
    }

    Schema readSchema =
        new Schema(
            required(100, "id", Types.LongType.get()),
            optional(101, "int_data", Types.LongType.get()),
            optional(102, "float_data", Types.DoubleType.get()),
            optional(103, "decimal_data", Types.DecimalType.of(25, 5)));

    assertRecordsMatch(readSchema, 30000, data, dataFile, false, BATCH_SIZE);
  }

  @Test
  public void testSupportedReadsForParquetV2() throws Exception {
    // Float and double column types are written using plain encoding with Parquet V2,
    // also Parquet V2 will dictionary encode decimals that use fixed length binary
    // (i.e. decimals > 8 bytes). Int and long types use DELTA_BINARY_PACKED.
    Schema schema =
        new Schema(
            optional(102, "float_data", Types.FloatType.get()),
            optional(103, "double_data", Types.DoubleType.get()),
            optional(104, "decimal_data", Types.DecimalType.of(25, 5)),
            optional(105, "int_data", Types.IntegerType.get()),
            optional(106, "long_data", Types.LongType.get()));

    File dataFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(dataFile.delete()).as("Delete should succeed").isTrue();
    Iterable<Record> data =
        generateData(schema, 30000, 0L, RandomData.DEFAULT_NULL_PERCENTAGE, IDENTITY);
    try (FileAppender<Record> writer = getParquetV2Writer(schema, dataFile)) {
      writer.addAll(data);
    }
    assertRecordsMatch(schema, 30000, data, dataFile, false, BATCH_SIZE);
  }

  @Test
  public void testUnsupportedReadsForParquetV2() throws Exception {
    // Some types use delta encoding and which are not supported for vectorized reads
    Schema schema = new Schema(SUPPORTED_PRIMITIVES.fields());
    File dataFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(dataFile.delete()).as("Delete should succeed").isTrue();
    Iterable<Record> data =
        generateData(schema, 30000, 0L, RandomData.DEFAULT_NULL_PERCENTAGE, IDENTITY);
    try (FileAppender<Record> writer = getParquetV2Writer(schema, dataFile)) {
      writer.addAll(data);
    }
    assertThatThrownBy(() -> assertRecordsMatch(schema, 30000, data, dataFile, false, BATCH_SIZE))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageStartingWith("Cannot support vectorized reads for column")
        .hasMessageEndingWith("Disable vectorized reads to read this table/file");
  }

  @Test
  public void testUuidReads() throws Exception {
    // Just one row to maintain dictionary encoding
    int numRows = 1;
    Schema schema = new Schema(optional(100, "uuid", Types.UUIDType.get()));

    File dataFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(dataFile.delete()).as("Delete should succeed").isTrue();
    Iterable<Record> data = generateData(schema, numRows, 0L, 0, IDENTITY);
    try (FileAppender<Record> writer = getParquetV2Writer(schema, dataFile)) {
      writer.addAll(data);
    }
    assertRecordsMatch(schema, numRows, data, dataFile, false, BATCH_SIZE);
  }

  private void assertIdenticalFileContents(
      File actual, File expected, Schema schema, boolean vectorized) throws IOException {
    try (CloseableIterable<Record> expectedIterator =
        Parquet.read(Files.localInput(expected))
            .project(schema)
            .createReaderFunc(msgType -> GenericParquetReaders.buildReader(schema, msgType))
            .build()) {
      List<Record> expectedRecords = Lists.newArrayList(expectedIterator);
      if (vectorized) {
        assertRecordsMatch(
            schema, expectedRecords.size(), expectedRecords, actual, false, BATCH_SIZE);
      } else {
        try (CloseableIterable<InternalRow> actualIterator =
            Parquet.read(Files.localInput(actual))
                .project(schema)
                .createReaderFunc(msgType -> SparkParquetReaders.buildReader(schema, msgType))
                .build()) {
          List<InternalRow> actualRecords = Lists.newArrayList(actualIterator);
          assertThat(actualRecords).hasSameSizeAs(expectedRecords);
          for (int i = 0; i < actualRecords.size(); i++) {
            GenericsHelpers.assertEqualsUnsafe(
                schema.asStruct(), expectedRecords.get(i), actualRecords.get(i));
          }
        }
      }
    }
  }

  static Stream<Arguments> goldenFilesAndEncodings() {
    return GOLDEN_FILE_ENCODINGS.stream()
        .flatMap(
            encoding ->
                GOLDEN_FILE_TYPES.entrySet().stream()
                    .flatMap(
                        e ->
                            Stream.of(true, false)
                                .map(
                                    vectorized ->
                                        Arguments.of(
                                            encoding, e.getKey(), e.getValue(), vectorized))));
  }

  private File resourceUrlToLocalFile(URL url) throws IOException, URISyntaxException {
    if ("file".equals(url.getProtocol())) {
      return Paths.get(url.toURI()).toFile();
    }

    String name = Paths.get(url.getPath()).getFileName().toString(); // e.g., string.parquet
    String suffix = name.contains(".") ? name.substring(name.lastIndexOf('.')) : "";
    File tmp = File.createTempFile("golden-", suffix, temp.toFile());
    try (InputStream in = url.openStream()) {
      java.nio.file.Files.copy(in, tmp.toPath(), REPLACE_EXISTING);
    }
    return tmp;
  }

  @ParameterizedTest
  @MethodSource("goldenFilesAndEncodings")
  public void testGoldenFiles(
      String encoding, String typeName, PrimitiveType primitiveType, boolean vectorized)
      throws Exception {
    Path goldenResourcePath = Paths.get("encodings", encoding, typeName + ".parquet");
    URL goldenFileUrl = getClass().getClassLoader().getResource(goldenResourcePath.toString());
    assumeThat(goldenFileUrl).as("type/encoding pair exists").isNotNull();

    Path plainResourcePath = Paths.get("encodings", PLAIN, typeName + ".parquet");
    URL plainFileUrl = getClass().getClassLoader().getResource(plainResourcePath.toString());
    Preconditions.checkState(
        plainFileUrl != null, "PLAIN encoded file should exist: " + plainResourcePath);

    Schema expectedSchema = new Schema(optional(1, "data", primitiveType));
    assertIdenticalFileContents(
        resourceUrlToLocalFile(goldenFileUrl),
        resourceUrlToLocalFile(plainFileUrl),
        expectedSchema,
        vectorized);
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

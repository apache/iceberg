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
package org.apache.iceberg.data;

import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;
import static org.apache.iceberg.TestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.MetricsModes.MetricsMode;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.formats.FileWriterBuilder;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;

public abstract class BaseFormatModelTests<T> {

  protected abstract Class<T> engineType();

  protected abstract Object engineSchema(Schema schema);

  protected abstract T convertToEngine(Record record, Schema schema);

  protected abstract void assertEquals(Schema schema, List<T> expected, List<T> actual);

  protected abstract Object convertConstantToEngine(Type type, Object value);

  protected boolean supportsBatchReads() {
    return false;
  }

  @TempDir private File tableDir;

  private static final FileFormat[] FILE_FORMATS =
      new FileFormat[] {FileFormat.AVRO, FileFormat.PARQUET, FileFormat.ORC};

  private static final List<Arguments> FORMAT_AND_GENERATOR =
      Arrays.stream(FILE_FORMATS)
          .flatMap(
              format ->
                  Arrays.stream(DataGenerators.ALL)
                      .map(generator -> Arguments.of(format, generator)))
          .toList();

  static final String FEATURE_FILTER = "filter";
  static final String FEATURE_CASE_SENSITIVE = "caseSensitive";
  static final String FEATURE_SPLIT = "split";
  static final String FEATURE_READER_DEFAULT = "readerDefault";
  static final String FEATURE_REUSE_CONTAINERS = "reuseContainers";
  static final String FEATURE_META_ROW_LINEAGE = "metaRowLineage";
  static final String FEATURE_COLUMN_LEVEL_METRICS = "columnLevelMetrics";
  static final String FEATURE_COLUMN_METRICS_TRUNCATE_BINARY = "columnMetricsTruncateBinary";

  private static final Map<FileFormat, String[]> MISSING_FEATURES =
      Map.of(
          FileFormat.AVRO,
          new String[] {
            FEATURE_FILTER,
            FEATURE_CASE_SENSITIVE,
            FEATURE_SPLIT,
            FEATURE_COLUMN_LEVEL_METRICS,
            FEATURE_COLUMN_METRICS_TRUNCATE_BINARY
          },
          FileFormat.ORC,
          new String[] {
            FEATURE_REUSE_CONTAINERS,
            FEATURE_COLUMN_METRICS_TRUNCATE_BINARY,
            FEATURE_META_ROW_LINEAGE,
            FEATURE_READER_DEFAULT
          });

  private InMemoryFileIO fileIO;
  private EncryptedOutputFile encryptedFile;

  @BeforeEach
  void before() {
    this.fileIO = new InMemoryFileIO();
    this.encryptedFile =
        EncryptedFiles.encryptedOutput(
            fileIO.newOutputFile("test-file-" + UUID.randomUUID()), EncryptionKeyMetadata.EMPTY);
  }

  @AfterEach
  void after() {
    try {
      fileIO.deleteFile(encryptedFile.encryptingOutputFile());
    } catch (NotFoundException ignored) {
      // ignore if the file is not created
    }

    this.encryptedFile = null;
    if (fileIO != null) {
      fileIO.close();
    }

    TestTables.clearTables();
  }

  /** Write with engine type T, read with Generic Record */
  @ParameterizedTest
  @FieldSource("FORMAT_AND_GENERATOR")
  void testDataWriterEngineWriteGenericRead(FileFormat fileFormat, DataGenerator dataGenerator)
      throws IOException {
    Schema schema = dataGenerator.schema();
    FileWriterBuilder<DataWriter<T>, Object> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, engineType(), encryptedFile);

    DataWriter<T> writer = writerBuilder.schema(schema).spec(PartitionSpec.unpartitioned()).build();

    List<Record> genericRecords = dataGenerator.generateRecords();
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);

    try (writer) {
      engineRecords.forEach(writer::write);
    }

    DataFile dataFile = writer.toDataFile();

    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(engineRecords.size());
    assertThat(dataFile.format()).isEqualTo(fileFormat);

    readAndAssertGenericRecords(fileFormat, schema, genericRecords);
  }

  /** Write with engine type T without explicit engineSchema, read with Generic Record */
  @ParameterizedTest
  @FieldSource("FORMAT_AND_GENERATOR")
  void testDataWriterEngineWriteWithoutEngineSchema(
      FileFormat fileFormat, DataGenerator dataGenerator) throws IOException {
    Schema schema = dataGenerator.schema();
    FileWriterBuilder<DataWriter<T>, Object> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, engineType(), encryptedFile);

    DataWriter<T> writer = writerBuilder.schema(schema).spec(PartitionSpec.unpartitioned()).build();

    List<Record> genericRecords = dataGenerator.generateRecords();
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);

    try (writer) {
      engineRecords.forEach(writer::write);
    }

    DataFile dataFile = writer.toDataFile();

    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(engineRecords.size());
    assertThat(dataFile.format()).isEqualTo(fileFormat);

    readAndAssertGenericRecords(fileFormat, schema, genericRecords);
  }

  /** Write with Generic Record, read with engine type T */
  @ParameterizedTest
  @FieldSource("FORMAT_AND_GENERATOR")
  void testDataWriterGenericWriteEngineRead(FileFormat fileFormat, DataGenerator dataGenerator)
      throws IOException {
    Schema schema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, schema, genericRecords);

    // Read back and verify
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<T> readRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertEquals(schema, convertToEngineRecords(genericRecords, schema), readRecords);
  }

  /** Write with engine type T, read with Generic Record */
  @ParameterizedTest
  @FieldSource("FORMAT_AND_GENERATOR")
  void testEqualityDeleteWriterEngineWriteGenericRead(
      FileFormat fileFormat, DataGenerator dataGenerator) throws IOException {
    Schema schema = dataGenerator.schema();
    FileWriterBuilder<EqualityDeleteWriter<T>, Object> writerBuilder =
        FormatModelRegistry.equalityDeleteWriteBuilder(fileFormat, engineType(), encryptedFile);

    EqualityDeleteWriter<T> writer =
        writerBuilder
            .schema(schema)
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(1)
            .build();

    List<Record> genericRecords = dataGenerator.generateRecords();
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);

    try (writer) {
      engineRecords.forEach(writer::write);
    }

    DeleteFile deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(engineRecords.size());
    assertThat(deleteFile.format()).isEqualTo(fileFormat);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(1);

    readAndAssertGenericRecords(fileFormat, schema, genericRecords);
  }

  /**
   * Write equality deletes with engine type T without explicit engineSchema, read with Generic
   * Record
   */
  @ParameterizedTest
  @FieldSource("FORMAT_AND_GENERATOR")
  void testEqualityDeleteWriterEngineWriteWithoutEngineSchema(
      FileFormat fileFormat, DataGenerator dataGenerator) throws IOException {
    Schema schema = dataGenerator.schema();
    FileWriterBuilder<EqualityDeleteWriter<T>, Object> writerBuilder =
        FormatModelRegistry.equalityDeleteWriteBuilder(fileFormat, engineType(), encryptedFile);

    EqualityDeleteWriter<T> writer =
        writerBuilder
            .schema(schema)
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(1)
            .build();

    List<Record> genericRecords = dataGenerator.generateRecords();
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);

    try (writer) {
      engineRecords.forEach(writer::write);
    }

    DeleteFile deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(engineRecords.size());
    assertThat(deleteFile.format()).isEqualTo(fileFormat);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(1);

    readAndAssertGenericRecords(fileFormat, schema, genericRecords);
  }

  /** Write with Generic Record, read with engine type T */
  @ParameterizedTest
  @FieldSource("FORMAT_AND_GENERATOR")
  void testEqualityDeleteWriterGenericWriteEngineRead(
      FileFormat fileFormat, DataGenerator dataGenerator) throws IOException {
    Schema schema = dataGenerator.schema();
    FileWriterBuilder<EqualityDeleteWriter<Record>, Object> writerBuilder =
        FormatModelRegistry.equalityDeleteWriteBuilder(fileFormat, Record.class, encryptedFile);

    EqualityDeleteWriter<Record> writer =
        writerBuilder
            .schema(schema)
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(1)
            .build();

    List<Record> genericRecords = dataGenerator.generateRecords();

    try (writer) {
      genericRecords.forEach(writer::write);
    }

    DeleteFile deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(genericRecords.size());
    assertThat(deleteFile.format()).isEqualTo(fileFormat);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(1);

    // Read back and verify
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<T> readRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertEquals(schema, convertToEngineRecords(genericRecords, schema), readRecords);
  }

  /** Write position deletes, read with Generic Record */
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testPositionDeleteWriterEngineWriteGenericRead(FileFormat fileFormat) throws IOException {
    Schema positionDeleteSchema = DeleteSchemaUtil.pathPosSchema();

    FileWriterBuilder<PositionDeleteWriter<T>, ?> writerBuilder =
        FormatModelRegistry.positionDeleteWriteBuilder(fileFormat, encryptedFile);

    List<PositionDelete<T>> deletes =
        ImmutableList.of(
            PositionDelete.<T>create().set("data-file-1.parquet", 0L),
            PositionDelete.<T>create().set("data-file-1.parquet", 1L));

    List<Record> records =
        deletes.stream()
            .map(
                d ->
                    GenericRecord.create(positionDeleteSchema)
                        .copy(DELETE_FILE_PATH.name(), d.path(), DELETE_FILE_POS.name(), d.pos()))
            .toList();

    PositionDeleteWriter<T> writer = writerBuilder.spec(PartitionSpec.unpartitioned()).build();
    try (writer) {
      deletes.forEach(writer::write);
    }

    DeleteFile deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(2);
    assertThat(deleteFile.format()).isEqualTo(fileFormat);

    readAndAssertGenericRecords(fileFormat, positionDeleteSchema, records);
  }

  /** Write with Generic Record, read with projected engine type T (narrow schema) */
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReaderBuilderProjection(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema fullSchema = dataGenerator.schema();

    List<Types.NestedField> columns = fullSchema.columns();
    List<Types.NestedField> projectedColumns =
        IntStream.range(0, columns.size()).filter(i -> i % 2 == 1).mapToObj(columns::get).toList();
    if (projectedColumns.isEmpty()) {
      projectedColumns = ImmutableList.of(columns.get(columns.size() - 1));
    }

    Schema projectedSchema = new Schema(projectedColumns);

    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, fullSchema, genericRecords);

    List<Record> projectedGenericRecords =
        genericRecords.stream()
            .map(record -> copy(record, projectedSchema, projectedSchema))
            .toList();
    List<T> expectedEngineRecords =
        convertToEngineRecords(projectedGenericRecords, projectedSchema);

    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<T> readRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(projectedSchema)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertEquals(projectedSchema, expectedEngineRecords, readRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReaderBuilderFilter(FileFormat fileFormat) throws IOException {

    assumeSupports(fileFormat, FEATURE_FILTER);

    Schema schema = SCHEMA;

    // Generate records with known id values [0, count)
    int count = 10000;
    List<Record> genericRecords =
        IntStream.range(0, count)
            .mapToObj(i -> GenericRecord.create(SCHEMA).copy("id", i, "data", "row-" + i))
            .toList();

    writeRecordsForSplit(fileFormat, schema, genericRecords);

    // Filter: id < 0, so no record matches, file-level filtering should eliminate all rows
    Expression lessThanFilter = Expressions.lessThan("id", 0);

    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<T> readRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .filter(lessThanFilter)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertThat(readRecords).isEmpty();

    // Filter: id >= count - 1, so only the last record matches across all row groups
    Expression greaterThanFilter = Expressions.greaterThanOrEqual("id", count - 1);

    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .filter(greaterThanFilter)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertThat(readRecords).hasSizeGreaterThan(0).hasSizeLessThan(genericRecords.size());
  }

  /**
   * Write with Generic Record, then read using an upper-cased column name in the filter to verify
   * caseSensitive behavior.
   */
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReaderBuilderCaseSensitive(FileFormat fileFormat) throws IOException {

    assumeSupports(fileFormat, FEATURE_CASE_SENSITIVE);

    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, schema, genericRecords);

    // Build a filter using the upper-cased name of the first column.
    Types.NestedField firstField = schema.columns().get(0);
    Object filterValue = genericRecords.get(0).getField(firstField.name());
    Expression upperCaseFilter = Expressions.equal(firstField.name().toUpperCase(), filterValue);
    assertThat(firstField.name()).isNotEqualTo(firstField.name().toUpperCase());

    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();

    // caseSensitive=false: upper-cased column name must be resolved correctly.
    List<T> readRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .filter(upperCaseFilter)
            .caseSensitive(false)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertThat(readRecords).isNotEmpty();

    // caseSensitive=true: upper-cased column name cannot be resolved, so must throw.
    assertThatThrownBy(
            () -> {
              try (CloseableIterable<T> reader =
                  FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
                      .project(schema)
                      .filter(upperCaseFilter)
                      .caseSensitive(true)
                      .build()) {
                ImmutableList.copyOf(reader);
              }
            })
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field '%s'", firstField.name().toUpperCase());
  }

  /**
   * Write with Generic Record, then read using split to verify that the split range is respected.
   * The test writes enough records to produce multiple split so that {@link
   * DataFile#splitOffsets()} contains at least two offsets. It then reads only the first row-group
   * split {@code [splitOffsets[0], splitOffsets[1])} and asserts that the returned record count is
   * greater than zero but less than the total, confirming that the split boundary is honoured. A
   * second read over the full file range {@code [0, fileLength)} must return all records.
   */
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReaderBuilderSplit(FileFormat fileFormat) throws IOException {

    assumeSupports(fileFormat, FEATURE_SPLIT);

    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();
    List<Record> records = dataGenerator.generateRecords(10000);

    DataFile dataFile = writeRecordsForSplit(fileFormat, schema, records);

    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    long fileLength = inputFile.getLength();

    List<Long> splitOffsets = dataFile.splitOffsets();

    long firstSplitStart = splitOffsets.get(0);
    long firstSplitLength = splitOffsets.get(1) - splitOffsets.get(0);

    List<T> readRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .split(firstSplitStart, firstSplitLength)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertThat(readRecords).hasSizeGreaterThan(0).hasSizeLessThan(records.size());

    // split(fileLength, 0): empty range at the end of the file, so no records should be returned
    List<T> emptyReadRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .split(fileLength, 0)
            .build()) {
      emptyReadRecords = ImmutableList.copyOf(reader);
    }

    assertThat(emptyReadRecords).isEmpty();

    // split(0, fileLength): full file range, so all records should be returned
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .split(0, fileLength)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertThat(readRecords).hasSize(records.size());
  }

  /** Verifies the contract of reuseContainers */
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReaderBuilderReuseContainers(FileFormat fileFormat) throws IOException {

    assumeSupports(fileFormat, FEATURE_REUSE_CONTAINERS);

    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();
    List<Record> genericRecords = dataGenerator.generateRecords();
    // Need at least 2 records to verify container reuse
    assumeThat(genericRecords).hasSizeGreaterThanOrEqualTo(2);
    writeGenericRecords(fileFormat, schema, genericRecords);

    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();

    // Without reuseContainers: every record must be a distinct object instance
    List<T> noReuseRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .build()) {
      noReuseRecords = ImmutableList.copyOf(reader);
    }

    for (int i = 0; i < noReuseRecords.size(); i++) {
      for (int j = i + 1; j < noReuseRecords.size(); j++) {
        assertThat(noReuseRecords.get(i)).isNotSameAs(noReuseRecords.get(j));
      }
    }

    // With reuseContainers: all collected elements must be the same object instance
    List<T> reuseRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .reuseContainers()
            .build()) {
      reuseRecords = ImmutableList.copyOf(reader);
    }

    reuseRecords.forEach(r -> assertThat(r).isSameAs(reuseRecords.get(0)));
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReaderSchemaEvolutionNewColumnWithDefault(FileFormat fileFormat) throws IOException {

    assumeSupports(fileFormat, FEATURE_READER_DEFAULT);
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema writeSchema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, writeSchema, genericRecords);

    String defaultStringValue = "default_value";
    int defaultIntValue = 42;

    int maxFieldId =
        writeSchema.columns().stream().mapToInt(Types.NestedField::fieldId).max().orElse(0);

    List<Types.NestedField> evolvedColumns = Lists.newArrayList(writeSchema.columns());
    evolvedColumns.add(
        Types.NestedField.required("col_f")
            .withId(maxFieldId + 1)
            .ofType(Types.StringType.get())
            .withInitialDefault(Literal.of(defaultStringValue))
            .build());
    evolvedColumns.add(
        Types.NestedField.optional("col_g")
            .withId(maxFieldId + 2)
            .ofType(Types.IntegerType.get())
            .withInitialDefault(Literal.of(defaultIntValue))
            .build());

    Schema evolvedSchema = new Schema(evolvedColumns);
    readAndAssertGenericRecords(
        fileFormat,
        evolvedSchema,
        genericRecords,
        record -> {
          Record expected = copy(record, writeSchema, evolvedSchema);
          expected.setField("col_f", defaultStringValue);
          expected.setField("col_g", defaultIntValue);
          return expected;
        });
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReaderBuilderRecordsPerBatchNotSupported(FileFormat fileFormat) throws IOException {
    assumeFalse(supportsBatchReads(), engineType().getSimpleName() + " supports batch reads");

    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();
    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, schema, genericRecords);

    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    assertThatThrownBy(
            () ->
                FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
                    .recordsPerBatch(100))
        .hasMessageContaining("Batch reading is not supported")
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterMetricsCollection(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords();
    DataFile dataFile = writeGenericRecords(fileFormat, schema, genericRecords);

    assertCounts(fileFormat, schema, genericRecords, dataFile);
    assertBounds(fileFormat, schema, genericRecords, dataFile);
    assertColumnSize(fileFormat, dataFile);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterMetricsWithNoneMode(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();

    MetricsConfig noneConfig = config(schema, MetricsModes.None.get());
    List<Record> genericRecords = dataGenerator.generateRecords();
    DataFile dataFile = writeGenericRecords(fileFormat, schema, genericRecords, noneConfig);

    assertCountsNull(schema, dataFile);
    assertBoundsNull(schema, dataFile);
    assertColumnSizeEmpty(fileFormat, dataFile);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterMetricsWithCountsMode(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();
    MetricsConfig countsConfig = config(schema, MetricsModes.Counts.get());
    List<Record> genericRecords = dataGenerator.generateRecords();
    DataFile dataFile = writeGenericRecords(fileFormat, schema, genericRecords, countsConfig);

    // In the counts mode, valueCounts and nullValueCounts should be present, while lowerBounds and
    // upperBounds should be null.
    assertCounts(fileFormat, schema, genericRecords, dataFile);
    assertBoundsNull(schema, dataFile);
    assertColumnSize(fileFormat, dataFile);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterMetricsWithTruncateMode(FileFormat fileFormat) throws IOException {
    int truncateLength = 5;
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "col_str", Types.StringType.get()),
            Types.NestedField.required(2, "col_int", Types.IntegerType.get()));

    List<Record> records = Lists.newArrayList();
    records.add(GenericRecord.create(schema).copy("col_str", "abcdefghij", "col_int", 10));
    records.add(GenericRecord.create(schema).copy("col_str", "abcdezyxwv", "col_int", 20));
    records.add(GenericRecord.create(schema).copy("col_str", "abcdeAAAAA", "col_int", 5));

    assertTruncateBoundsForFirstColumn(
        fileFormat,
        schema,
        records,
        truncateLength,
        FEATURE_COLUMN_LEVEL_METRICS,
        (lower, upper) -> {
          // Lower bound: "abcdeAAAAA" truncated to "abcde"
          CharSequence actualLower = Conversions.fromByteBuffer(Types.StringType.get(), lower);
          assertThat(actualLower.toString()).hasSize(truncateLength);
          assertThat(actualLower.toString()).isEqualTo("abcde");

          // Upper bound: "abcdezyxwv" truncated and incremented to "abcdf"
          CharSequence actualUpper = Conversions.fromByteBuffer(Types.StringType.get(), upper);
          assertThat(actualUpper.toString()).hasSize(truncateLength);
          assertThat(actualUpper.toString()).isEqualTo("abcdf");
        });
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterMetricsWithTruncateModeForBinary(FileFormat fileFormat) throws IOException {
    int truncateLength = 5;
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "col_bin", Types.BinaryType.get()),
            Types.NestedField.required(2, "col_int", Types.IntegerType.get()));

    List<Record> records = Lists.newArrayList();
    records.add(
        GenericRecord.create(schema)
            .copy(
                "col_bin",
                ByteBuffer.wrap(
                    new byte[] {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0x10, 0xA, 0xB}),
                "col_int",
                10));

    assertTruncateBoundsForFirstColumn(
        fileFormat,
        schema,
        records,
        truncateLength,
        FEATURE_COLUMN_METRICS_TRUNCATE_BINARY,
        (lower, upper) -> {
          ByteBuffer actualLower = Conversions.fromByteBuffer(Types.BinaryType.get(), lower);
          ByteBuffer actualUpper = Conversions.fromByteBuffer(Types.BinaryType.get(), upper);

          ByteBuffer expectedLower = ByteBuffer.wrap(new byte[] {0x1, 0x2, 0x3, 0x4, 0x5});
          ByteBuffer expectedUpper = ByteBuffer.wrap(new byte[] {0x1, 0x2, 0x3, 0x4, 0x6});

          assertThat(actualLower).isEqualTo(expectedLower);
          assertThat(actualUpper).isEqualTo(expectedUpper);
        });
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testEqualityDeleteWriterMetricsCollection(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();
    FileWriterBuilder<EqualityDeleteWriter<Record>, Object> writerBuilder =
        FormatModelRegistry.equalityDeleteWriteBuilder(fileFormat, Record.class, encryptedFile);

    EqualityDeleteWriter<Record> writer =
        writerBuilder
            .schema(schema)
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(1)
            .build();

    List<Record> genericRecords = dataGenerator.generateRecords();

    try (writer) {
      genericRecords.forEach(writer::write);
    }

    DeleteFile deleteFile = writer.toDeleteFile();

    assertCounts(fileFormat, schema, genericRecords, deleteFile);
    assertBounds(fileFormat, schema, genericRecords, deleteFile);
    assertColumnSize(fileFormat, deleteFile);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testPositionDeleteWriterMetricsSingleFile(FileFormat fileFormat) throws IOException {
    // Single file reference: counts are removed but bounds are preserved.
    List<PositionDelete<T>> deletes =
        ImmutableList.of(
            PositionDelete.<T>create().set("d-file-1.file", 0L),
            PositionDelete.<T>create().set("d-file-1.file", 5L),
            PositionDelete.<T>create().set("d-file-1.file", 3L));

    DeleteFile deleteFile = writePositionDeletes(fileFormat, deletes);
    assertPositionDeleteMetrics(fileFormat, deletes, deleteFile, true /* checkBounds */);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testPositionDeleteWriterMetricsMultipleFiles(FileFormat fileFormat) throws IOException {
    // Multiple file references: both counts and bounds are removed.
    List<PositionDelete<T>> deletes =
        ImmutableList.of(
            PositionDelete.<T>create().set("d-file-1.file", 0L),
            PositionDelete.<T>create().set("d-file-1.file", 5L),
            PositionDelete.<T>create().set("d-file-2.file", 3L));

    DeleteFile deleteFile = writePositionDeletes(fileFormat, deletes);
    assertPositionDeleteMetrics(fileFormat, deletes, deleteFile, false /* checkBounds */);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterMetricsWithPerColumnMode(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();

    // Default mode is "counts", col_b is overridden to "full", col_a is overridden to "none"
    MetricsConfig perColumnConfig =
        config(
            schema,
            MetricsModes.Counts.get(),
            ImmutableMap.of("col_b", MetricsModes.Full.get(), "col_a", MetricsModes.None.get()));

    List<Record> genericRecords = dataGenerator.generateRecords();
    DataFile dataFile = writeGenericRecords(fileFormat, schema, genericRecords, perColumnConfig);

    // col_a: mode=none -> no valueCounts, nullValueCounts, bounds
    Schema noneSchema = new Schema(schema.findField("col_a"));
    assertCountsNull(noneSchema, dataFile);
    assertBoundsNull(noneSchema, dataFile);

    // col_b: mode=full -> valueCounts, nullValueCounts, and bounds all present
    Schema fullSchema = new Schema(schema.findField("col_b"));
    assertCounts(fileFormat, fullSchema, genericRecords, dataFile);
    assertBounds(fileFormat, fullSchema, genericRecords, dataFile);

    // col_c, col_d, col_e: mode=counts (default) -> valueCounts and nullValueCounts present,
    // but no bounds
    Schema countsSchema =
        new Schema(schema.findField("col_c"), schema.findField("col_d"), schema.findField("col_e"));
    assertCounts(fileFormat, countsSchema, genericRecords, dataFile);
    assertBoundsNull(countsSchema, dataFile);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterNanMetrics(FileFormat fileFormat) throws IOException {
    Schema schema = new DataGenerators.FloatDoubleSchema().schema();

    List<Record> records = Lists.newArrayList();
    records.add(
        GenericRecord.create(schema).copy("col_float", Float.NaN, "col_double", Double.NaN));
    records.add(
        GenericRecord.create(schema).copy("col_float", Float.NaN, "col_double", Double.NaN));
    records.add(GenericRecord.create(schema).copy("col_float", 1.0F, "col_double", 10.0D));
    records.add(GenericRecord.create(schema).copy("col_float", 5.0F, "col_double", 50.0D));
    records.add(GenericRecord.create(schema).copy("col_float", 3.0F, "col_double", 30.0D));

    DataFile dataFile = writeGenericRecords(fileFormat, schema, records);

    assertCounts(fileFormat, schema, records, dataFile);
    assertBounds(fileFormat, schema, records, dataFile);
    assertNanCounts(fileFormat, schema, records, dataFile);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterNanSortingOrder(FileFormat fileFormat) throws IOException {
    Schema schema = new DataGenerators.FloatDoubleSchema().schema();

    List<Record> records = Lists.newArrayList();
    records.add(
        GenericRecord.create(schema).copy("col_float", Float.NaN, "col_double", Double.NaN));
    records.add(
        GenericRecord.create(schema)
            .copy("col_float", Float.NEGATIVE_INFINITY, "col_double", Double.NEGATIVE_INFINITY));
    records.add(GenericRecord.create(schema).copy("col_float", -1.0F, "col_double", -1.0D));
    records.add(GenericRecord.create(schema).copy("col_float", -0.0F, "col_double", -0.0D));
    records.add(GenericRecord.create(schema).copy("col_float", 0.0F, "col_double", 0.0D));
    records.add(GenericRecord.create(schema).copy("col_float", 1.0F, "col_double", 1.0D));
    records.add(
        GenericRecord.create(schema)
            .copy("col_float", Float.POSITIVE_INFINITY, "col_double", Double.POSITIVE_INFINITY));
    records.add(
        GenericRecord.create(schema).copy("col_float", Float.NaN, "col_double", Double.NaN));

    DataFile dataFile = writeGenericRecords(fileFormat, schema, records);

    // Bounds should exclude NaN: float/double lower = -Infinity, upper = +Infinity
    assertBounds(fileFormat, schema, records, dataFile);
    assertNanCounts(fileFormat, schema, records, dataFile);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterNegativeZeroBounds(FileFormat fileFormat) throws IOException {
    Schema schema = new DataGenerators.FloatDoubleSchema().schema();

    List<Record> records = Lists.newArrayList();
    records.add(GenericRecord.create(schema).copy("col_float", -0.0F, "col_double", -0.0D));
    records.add(GenericRecord.create(schema).copy("col_float", 0.0F, "col_double", 0.0D));

    DataFile dataFile = writeGenericRecords(fileFormat, schema, records);
    assertBounds(fileFormat, schema, records, dataFile);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReadMetadataColumnFilePath(FileFormat fileFormat) throws IOException {

    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();
    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, schema, genericRecords);

    String filePath = "test-data-file.parquet";
    Schema projectionSchema = new Schema(MetadataColumns.FILE_PATH);

    Map<Integer, Object> idToConstant =
        ImmutableMap.of(MetadataColumns.FILE_PATH.fieldId(), filePath);

    readAndAssertMetadataColumn(
        fileFormat,
        projectionSchema,
        idToConstant,
        genericRecords,
        ignored ->
            GenericRecord.create(projectionSchema)
                .copy(MetadataColumns.FILE_PATH.name(), filePath));
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReadMetadataColumnSpecId(FileFormat fileFormat) throws IOException {

    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();
    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, schema, genericRecords);

    int specId = 0;
    Schema projectionSchema = new Schema(MetadataColumns.SPEC_ID);

    Map<Integer, Object> idToConstant = ImmutableMap.of(MetadataColumns.SPEC_ID.fieldId(), specId);

    readAndAssertMetadataColumn(
        fileFormat,
        projectionSchema,
        idToConstant,
        genericRecords,
        ignored ->
            GenericRecord.create(projectionSchema).copy(MetadataColumns.SPEC_ID.name(), specId));
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReadMetadataColumnRowPosition(FileFormat fileFormat) throws IOException {

    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();
    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, schema, genericRecords);

    Schema projectionSchema = new Schema(MetadataColumns.ROW_POSITION);

    readAndAssertMetadataColumn(
        fileFormat,
        projectionSchema,
        null,
        genericRecords,
        (position, ignored) ->
            GenericRecord.create(projectionSchema)
                .copy(MetadataColumns.ROW_POSITION.name(), (long) position));
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReadMetadataColumnIsDeleted(FileFormat fileFormat) throws IOException {

    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();
    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, schema, genericRecords);

    Schema projectionSchema = new Schema(MetadataColumns.IS_DELETED);

    readAndAssertMetadataColumn(
        fileFormat,
        projectionSchema,
        null,
        genericRecords,
        ignored ->
            GenericRecord.create(projectionSchema).copy(MetadataColumns.IS_DELETED.name(), false));
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReadMetadataColumnRowLinage(FileFormat fileFormat) throws IOException {
    assumeSupports(fileFormat, FEATURE_META_ROW_LINEAGE);

    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();
    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, schema, genericRecords);

    long baseRowId = 100L;
    long fileSeqNumber = 5L;
    Schema projectionSchema =
        new Schema(MetadataColumns.ROW_ID, MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);

    Map<Integer, Object> idToConstant =
        ImmutableMap.of(
            MetadataColumns.ROW_ID.fieldId(), baseRowId,
            MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(), fileSeqNumber);

    readAndAssertMetadataColumn(
        fileFormat,
        projectionSchema,
        idToConstant,
        genericRecords,
        (position, ignored) ->
            GenericRecord.create(projectionSchema)
                .copy(
                    MetadataColumns.ROW_ID.name(),
                    baseRowId + position,
                    MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(),
                    fileSeqNumber));
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReadMetadataColumnRowLinageExistValue(FileFormat fileFormat) throws IOException {
    assumeSupports(fileFormat, FEATURE_META_ROW_LINEAGE);

    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema dataSchema = dataGenerator.schema();

    Schema writeSchema = MetadataColumns.schemaWithRowLineage(dataSchema);

    List<Record> baseRecords = dataGenerator.generateRecords();
    List<Record> writeRecords = Lists.newArrayListWithExpectedSize(baseRecords.size());
    for (int i = 0; i < baseRecords.size(); i++) {
      Record base = baseRecords.get(i);
      Record rec = copy(base, dataSchema, writeSchema);

      if (i % 2 == 0) {
        rec.setField(MetadataColumns.ROW_ID.name(), 555L + i);
        rec.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), 7L);
      } else {
        rec.setField(MetadataColumns.ROW_ID.name(), null);
        rec.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), null);
      }

      writeRecords.add(rec);
    }

    DataWriter<Record> writer =
        FormatModelRegistry.dataWriteBuilder(fileFormat, Record.class, encryptedFile)
            .schema(writeSchema)
            .spec(PartitionSpec.unpartitioned())
            .build();

    try (writer) {
      writeRecords.forEach(writer::write);
    }

    long baseRowId = 100L;
    long fileSeqNumber = 5L;
    Schema projectionSchema =
        new Schema(MetadataColumns.ROW_ID, MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);

    Map<Integer, Object> idToConstant =
        ImmutableMap.of(
            MetadataColumns.ROW_ID.fieldId(), baseRowId,
            MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(), fileSeqNumber);

    // Expected results:
    // - Even rows (explicit values): _row_id = 555+i, _last_updated_sequence_number = 7
    // - Odd rows (null values): _row_id = baseRowId+pos, _last_updated_sequence_number =
    // fileSeqNumber
    readAndAssertMetadataColumn(
        fileFormat,
        projectionSchema,
        idToConstant,
        baseRecords,
        (position, ignored) -> {
          if (position % 2 == 0) {
            return GenericRecord.create(projectionSchema)
                .copy(
                    MetadataColumns.ROW_ID.name(),
                    555L + position,
                    MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(),
                    7L);
          } else {
            return GenericRecord.create(projectionSchema)
                .copy(
                    MetadataColumns.ROW_ID.name(),
                    baseRowId + position,
                    MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(),
                    fileSeqNumber);
          }
        });
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReadMetadataColumnPartitionIdentity(FileFormat fileFormat) throws IOException {

    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    PartitionSpec spec = PartitionSpec.builderFor(dataGenerator.schema()).identity("col_a").build();

    Types.StructType partitionType = spec.partitionType();
    PartitionData partitionData = new PartitionData(partitionType);
    partitionData.set(0, "test_col_a");

    DataWriter<Record> writer =
        FormatModelRegistry.dataWriteBuilder(fileFormat, Record.class, encryptedFile)
            .schema(dataGenerator.schema())
            .spec(PartitionSpec.unpartitioned())
            .build();

    List<Record> records = dataGenerator.generateRecords();
    try (writer) {
      records.forEach(writer::write);
    }

    Types.NestedField partitionField =
        Types.NestedField.optional(
            MetadataColumns.PARTITION_COLUMN_ID,
            MetadataColumns.PARTITION_COLUMN_NAME,
            partitionType,
            MetadataColumns.PARTITION_COLUMN_DOC);
    Schema projectionSchema = new Schema(partitionField);

    Map<Integer, Object> idToConstant =
        ImmutableMap.of(MetadataColumns.PARTITION_COLUMN_ID, partitionData);

    Record partitionRecord = structLikeToRecord(partitionData, partitionType);
    readAndAssertMetadataColumn(
        fileFormat,
        projectionSchema,
        idToConstant,
        records,
        ignored ->
            GenericRecord.create(projectionSchema)
                .copy(MetadataColumns.PARTITION_COLUMN_NAME, partitionRecord));
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReadMetadataColumnPartitionEvolutionAddColumn(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema dataSchema = dataGenerator.schema();

    // Old spec: partition by col_a only (spec id = 0)
    PartitionSpec oldSpec = PartitionSpec.builderFor(dataSchema).identity("col_a").build();

    // New spec: partition by col_a + col_b (spec id = 1, simulates partition evolution)
    PartitionSpec newSpec =
        PartitionSpec.builderFor(dataSchema)
            .withSpecId(1)
            .identity("col_a")
            .identity("col_b")
            .build();

    // Partition data for the old file (only col_a is set, col_b is absent)
    PartitionData oldPartitionData = new PartitionData(oldSpec.partitionType());
    oldPartitionData.set(0, "test_data");

    // Write data using the old spec
    DataWriter<Record> writer =
        FormatModelRegistry.dataWriteBuilder(fileFormat, Record.class, encryptedFile)
            .schema(dataSchema)
            .spec(PartitionSpec.unpartitioned())
            .build();

    List<Record> records = dataGenerator.generateRecords();

    try (writer) {
      records.forEach(writer::write);
    }

    Types.StructType unifiedPartitionType = newSpec.partitionType();

    // Build projection schema with PARTITION_COLUMN using the unified partition type
    Types.NestedField partitionField =
        Types.NestedField.optional(
            MetadataColumns.PARTITION_COLUMN_ID,
            MetadataColumns.PARTITION_COLUMN_NAME,
            unifiedPartitionType,
            MetadataColumns.PARTITION_COLUMN_DOC);
    Schema projectionSchema = new Schema(partitionField);

    Map<Integer, Object> idToConstant =
        ImmutableMap.of(MetadataColumns.PARTITION_COLUMN_ID, oldPartitionData);

    Record partitionRecord = structLikeToRecord(oldPartitionData, unifiedPartitionType);
    readAndAssertMetadataColumn(
        fileFormat,
        projectionSchema,
        idToConstant,
        records,
        ignored ->
            GenericRecord.create(projectionSchema)
                .copy(MetadataColumns.PARTITION_COLUMN_NAME, partitionRecord));
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReadMetadataColumnPartitionEvolutionRemoveColumn(FileFormat fileFormat)
      throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema dataSchema = dataGenerator.schema();

    PartitionSpec oldSpec =
        PartitionSpec.builderFor(dataSchema).identity("col_a").identity("col_b").build();

    PartitionSpec newSpec =
        PartitionSpec.builderFor(dataSchema).withSpecId(1).identity("col_a").build();

    // Partition data for the old file (both col_a and col_b are set)
    PartitionData oldPartitionData = new PartitionData(oldSpec.partitionType());
    oldPartitionData.set(0, "test_col_a");
    oldPartitionData.set(1, 1);

    DataWriter<Record> writer =
        FormatModelRegistry.dataWriteBuilder(fileFormat, Record.class, encryptedFile)
            .schema(dataSchema)
            .spec(PartitionSpec.unpartitioned())
            .build();

    List<Record> records = dataGenerator.generateRecords();

    try (writer) {
      records.forEach(writer::write);
    }

    // Use the new spec's partition type for projection (only col_a remains after evolution)
    // This simulates reading an old file from the perspective of the new spec
    Types.StructType newPartitionType = newSpec.partitionType();
    Types.NestedField partitionField =
        Types.NestedField.optional(
            MetadataColumns.PARTITION_COLUMN_ID,
            MetadataColumns.PARTITION_COLUMN_NAME,
            newPartitionType,
            MetadataColumns.PARTITION_COLUMN_DOC);
    Schema projectionSchema = new Schema(partitionField);

    Map<Integer, Object> idToConstant =
        ImmutableMap.of(MetadataColumns.PARTITION_COLUMN_ID, oldPartitionData);

    Record partitionRecord = structLikeToRecord(oldPartitionData, newPartitionType);
    readAndAssertMetadataColumn(
        fileFormat,
        projectionSchema,
        idToConstant,
        records,
        ignored ->
            GenericRecord.create(projectionSchema)
                .copy(MetadataColumns.PARTITION_COLUMN_NAME, partitionRecord));
  }

  private void readAndAssertGenericRecords(
      FileFormat fileFormat,
      Schema schema,
      List<Record> sourceRecords,
      Function<Record, Record> transform)
      throws IOException {
    readAndAssertGenericRecords(fileFormat, schema, sourceRecords.stream().map(transform).toList());
  }

  private void readAndAssertGenericRecords(
      FileFormat fileFormat, Schema schema, List<Record> expected) throws IOException {
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<Record> readRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, inputFile)
            .project(schema)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    DataTestHelpers.assertEquals(schema.asStruct(), expected, readRecords);
  }

  private DataFile writeGenericRecords(FileFormat fileFormat, Schema schema, List<Record> records)
      throws IOException {
    return writeGenericRecords(fileFormat, schema, records, null);
  }

  private DataFile writeGenericRecords(
      FileFormat fileFormat, Schema schema, List<Record> records, MetricsConfig metricsConfig)
      throws IOException {
    FileWriterBuilder<DataWriter<Record>, Object> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, Record.class, encryptedFile);

    if (metricsConfig != null) {
      writerBuilder.metricsConfig(metricsConfig);
    }

    DataWriter<Record> writer =
        writerBuilder.schema(schema).spec(PartitionSpec.unpartitioned()).build();

    try (writer) {
      records.forEach(writer::write);
    }

    DataFile dataFile = writer.toDataFile();
    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(records.size());
    assertThat(dataFile.format()).isEqualTo(fileFormat);

    return dataFile;
  }

  private List<T> convertToEngineRecords(List<Record> records, Schema schema) {
    return records.stream().map(r -> convertToEngine(r, schema)).toList();
  }

  private static void assumeSupports(FileFormat fileFormat, String feature) {
    assumeThat(MISSING_FEATURES.getOrDefault(fileFormat, new String[] {})).doesNotContain(feature);
  }

  /**
   * Returns whether the given file format supports the specified feature.
   *
   * <p>The check is based on {@link #MISSING_FEATURES}. Features not listed as missing for a format
   * are treated as supported.
   *
   * <p>Prefer this method over {@link #assumeSupports(FileFormat, String)} when only part of a test
   * should be skipped conditionally. Unlike {@code assumeSupports}, this method does not abort the
   * entire test via an assumption failure; it returns {@code false} so callers can skip only
   * feature-specific assertions while still validating shared behavior.
   *
   * @param fileFormat the file format under test
   * @param feature the feature name
   * @return {@code true} if the feature is supported by the format; {@code false} otherwise
   */
  private static boolean supportsFeature(FileFormat fileFormat, String feature) {
    String[] missing = MISSING_FEATURES.getOrDefault(fileFormat, new String[] {});
    return !Arrays.asList(missing).contains(feature);
  }

  private DataFile writeRecordsForSplit(FileFormat fileFormat, Schema schema, List<Record> records)
      throws IOException {

    String splitSizeProperty = splitSizeProperty(fileFormat);
    DataWriter<Record> writer =
        FormatModelRegistry.dataWriteBuilder(fileFormat, Record.class, encryptedFile)
            .schema(schema)
            .spec(PartitionSpec.unpartitioned())
            .set(splitSizeProperty, "1")
            .build();

    try (writer) {
      records.forEach(writer::write);
    }

    DataFile dataFile = writer.toDataFile();
    List<Long> splitOffsets = dataFile.splitOffsets();
    assertThat(splitOffsets)
        .as(
            "Expected multiple split offsets. "
                + "If this fails, the file did not produce multiple splits. "
                + "Try reducing the split size property (see writeRecordsForSplit) "
                + "or increasing the number of records written.")
        .hasSizeGreaterThan(1);

    assertThat(dataFile.format()).isEqualTo(fileFormat);
    return dataFile;
  }

  private static String splitSizeProperty(FileFormat fileFormat) {
    return switch (fileFormat) {
      case PARQUET -> TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
      case ORC -> TableProperties.ORC_STRIPE_SIZE_BYTES;
      default ->
          throw new UnsupportedOperationException(
              "No split size property defined for format: " + fileFormat);
    };
  }

  private static void assertCounts(
      FileFormat fileFormat, Schema schema, List<Record> genericRecords, ContentFile<?> file) {
    if (!supportsFeature(fileFormat, FEATURE_COLUMN_LEVEL_METRICS)) {
      return;
    }

    Map<Integer, Long> valueCounts = file.valueCounts();
    Map<Integer, Long> nullValueCounts = file.nullValueCounts();
    for (Types.NestedField field : schema.columns()) {
      if (field.type().isPrimitiveType()) {
        assertThat(valueCounts).containsKey(field.fieldId());
        assertThat(nullValueCounts).containsKey(field.fieldId());

        long nullCount =
            genericRecords.stream().filter(r -> r.getField(field.name()) == null).count();

        assertThat(valueCounts.get(field.fieldId())).isEqualTo(genericRecords.size());
        assertThat(nullValueCounts.get(field.fieldId())).isEqualTo(nullCount);
      }
    }
  }

  private static void assertBounds(
      FileFormat fileFormat, Schema schema, List<Record> genericRecords, ContentFile<?> file) {
    if (!supportsFeature(fileFormat, FEATURE_COLUMN_LEVEL_METRICS)) {
      return;
    }

    Map<Integer, ByteBuffer> lowerBounds = file.lowerBounds();
    Map<Integer, ByteBuffer> upperBounds = file.upperBounds();
    for (Types.NestedField field : schema.columns()) {
      if (field.type().isPrimitiveType()) {
        assertThat(lowerBounds).containsKey(field.fieldId());
        assertThat(upperBounds).containsKey(field.fieldId());

        ByteBuffer lowerBuffer = lowerBounds.get(field.fieldId());
        ByteBuffer upperBuffer = upperBounds.get(field.fieldId());

        Comparator<Object> cmp = Comparators.forType(field.type().asPrimitiveType());

        Object[] minMax = computeMinMax(genericRecords, field, cmp);
        Object expectedMin = minMax[0];
        Object expectedMax = minMax[1];

        if (expectedMin != null) {
          assertThat(lowerBuffer).isNotNull();
          Object actualLower = Conversions.fromByteBuffer(field.type(), lowerBuffer);
          assertThat(cmp.compare(actualLower, expectedMin)).isEqualTo(0);
        }

        if (expectedMax != null) {
          assertThat(upperBuffer).isNotNull();
          Object actualUpper = Conversions.fromByteBuffer(field.type(), upperBuffer);
          assertThat(cmp.compare(actualUpper, expectedMax)).isEqualTo(0);
        }
      }
    }
  }

  private static Object[] computeMinMax(
      List<Record> records, Types.NestedField field, Comparator<Object> cmp) {
    Object min = null;
    Object max = null;
    for (Record record : records) {
      Object value = record.getField(field.name());
      if (value == null) {
        continue;
      }

      if (value instanceof Float && ((Float) value).isNaN()) {
        continue;
      }

      if (value instanceof Double && ((Double) value).isNaN()) {
        continue;
      }

      if (min == null || cmp.compare(value, min) < 0) {
        min = value;
      }

      if (max == null || cmp.compare(value, max) > 0) {
        max = value;
      }
    }

    return new Object[] {min, max};
  }

  private static void assertBoundsNull(Schema schema, ContentFile<?> file) {
    Map<Integer, ByteBuffer> lowerBounds = file.lowerBounds();
    Map<Integer, ByteBuffer> upperBounds = file.upperBounds();
    for (Types.NestedField field : schema.columns()) {
      if (field.type().isPrimitiveType()) {
        assertThat(lowerBounds == null || lowerBounds.get(field.fieldId()) == null).isTrue();
        assertThat(upperBounds == null || upperBounds.get(field.fieldId()) == null).isTrue();
      }
    }
  }

  private static void assertColumnSize(FileFormat fileFormat, ContentFile<?> file) {
    if (!supportsFeature(fileFormat, FEATURE_COLUMN_LEVEL_METRICS)) {
      return;
    }

    assertThat(file.columnSizes()).isNotNull().isNotEmpty();
  }

  private static void assertColumnSizeEmpty(FileFormat fileFormat, ContentFile<?> file) {
    if (!supportsFeature(fileFormat, FEATURE_COLUMN_LEVEL_METRICS)) {
      return;
    }

    assertThat(file.columnSizes()).isEmpty();
  }

  private static void assertCountsNull(Schema schema, ContentFile<?> file) {
    Map<Integer, Long> valueCounts = file.valueCounts();
    Map<Integer, Long> nullValueCounts = file.nullValueCounts();
    for (Types.NestedField field : schema.columns()) {
      if (field.type().isPrimitiveType()) {
        assertThat(valueCounts == null || valueCounts.get(field.fieldId()) == null).isTrue();
        assertThat(nullValueCounts == null || nullValueCounts.get(field.fieldId()) == null)
            .isTrue();
      }
    }
  }

  private static void assertNanCounts(
      FileFormat fileFormat, Schema schema, List<Record> records, ContentFile<?> file) {
    if (!supportsFeature(fileFormat, FEATURE_COLUMN_LEVEL_METRICS)) {
      return;
    }

    Map<Integer, Long> nanValueCounts = file.nanValueCounts();
    assertThat(nanValueCounts).isNotNull();

    for (Types.NestedField field : schema.columns()) {
      if (field.type().typeId() == Type.TypeID.FLOAT
          || field.type().typeId() == Type.TypeID.DOUBLE) {
        long expectedNanCount =
            records.stream()
                .map(r -> r.getField(field.name()))
                .filter(
                    v ->
                        (v instanceof Float && ((Float) v).isNaN())
                            || (v instanceof Double && ((Double) v).isNaN()))
                .count();
        assertThat(nanValueCounts.get(field.fieldId())).isEqualTo(expectedNanCount);
      }
    }
  }

  private DeleteFile writePositionDeletes(FileFormat fileFormat, List<PositionDelete<T>> deletes)
      throws IOException {
    FileWriterBuilder<PositionDeleteWriter<T>, ?> writerBuilder =
        FormatModelRegistry.positionDeleteWriteBuilder(fileFormat, encryptedFile);

    PositionDeleteWriter<T> writer = writerBuilder.spec(PartitionSpec.unpartitioned()).build();
    try (writer) {
      deletes.forEach(writer::write);
    }

    return writer.toDeleteFile();
  }

  private void assertPositionDeleteMetrics(
      FileFormat fileFormat,
      List<PositionDelete<T>> deletes,
      DeleteFile deleteFile,
      boolean checkBounds) {
    Schema positionDeleteSchema = DeleteSchemaUtil.pathPosSchema();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(deletes.size());
    assertCountsNull(positionDeleteSchema, deleteFile);

    assumeSupports(fileFormat, FEATURE_COLUMN_LEVEL_METRICS);

    if (checkBounds) {
      // Single file reference: bounds are preserved
      List<Record> genericRecords =
          deletes.stream()
              .map(
                  d ->
                      GenericRecord.create(positionDeleteSchema)
                          .copy(
                              DELETE_FILE_PATH.name(), d.path(),
                              DELETE_FILE_POS.name(), d.pos()))
              .toList();
      assertBounds(fileFormat, positionDeleteSchema, genericRecords, deleteFile);
    } else {
      // Multiple file references: bounds are also removed
      assertBoundsNull(positionDeleteSchema, deleteFile);
    }
  }

  private MetricsConfig config(Schema schema, MetricsMode defaultMode) {
    return config(schema, defaultMode, ImmutableMap.of());
  }

  private MetricsConfig config(
      Schema schema, MetricsMode defaultMode, Map<String, MetricsMode> columnModes) {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    properties.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, defaultMode.toString());
    columnModes.forEach(
        (column, mode) ->
            properties.put(
                TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + column, mode.toString()));

    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", schema, PartitionSpec.unpartitioned(), 3, properties.build());

    return MetricsConfig.forTable(table);
  }

  private void assertTruncateBoundsForFirstColumn(
      FileFormat fileFormat,
      Schema schema,
      List<Record> records,
      int truncateLength,
      String requiredFeature,
      BiConsumer<ByteBuffer, ByteBuffer> boundsAssertion)
      throws IOException {
    MetricsConfig truncateConfig = config(schema, MetricsModes.Truncate.withLength(truncateLength));

    DataFile dataFile = writeGenericRecords(fileFormat, schema, records, truncateConfig);
    assertCounts(fileFormat, schema, records, dataFile);

    if (!supportsFeature(fileFormat, requiredFeature)) {
      return;
    }

    Map<Integer, ByteBuffer> lowerBounds = dataFile.lowerBounds();
    Map<Integer, ByteBuffer> upperBounds = dataFile.upperBounds();

    assertThat(lowerBounds).containsKey(1);
    assertThat(upperBounds).containsKey(1);

    boundsAssertion.accept(lowerBounds.get(1), upperBounds.get(1));

    Schema intSchema = new Schema(schema.findField("col_int"));
    assertBounds(fileFormat, intSchema, records, dataFile);

    assertThat(dataFile.columnSizes()).isNotNull().isNotEmpty();
  }

  private Map<Integer, Object> convertConstantsToEngine(
      Schema projectionSchema, Map<Integer, Object> idToConstant) {
    return idToConstant.entrySet().stream()
        .collect(
            ImmutableMap.toImmutableMap(
                Map.Entry::getKey,
                entry ->
                    convertConstantToEngine(
                        projectionSchema.findType(entry.getKey()), entry.getValue())));
  }

  private static Record structLikeToRecord(StructLike structLike, Types.StructType structType) {
    Record record = GenericRecord.create(structType);
    int sourceSize = structLike.size();
    for (int i = 0; i < structType.fields().size(); i++) {
      if (i < sourceSize) {
        record.set(i, structLike.get(i, Object.class));
      } else {
        Types.NestedField field = structType.fields().get(i);
        record.set(i, field.initialDefault());
      }
    }

    return record;
  }

  private void readAndAssertMetadataColumn(
      FileFormat fileFormat,
      Schema projectionSchema,
      Map<Integer, Object> idToConstant,
      List<Record> sourceRecords,
      Function<Record, Record> transform)
      throws IOException {
    readAndAssertMetadataColumn(
        fileFormat, projectionSchema, idToConstant, sourceRecords.stream().map(transform).toList());
  }

  private void readAndAssertMetadataColumn(
      FileFormat fileFormat,
      Schema projectionSchema,
      Map<Integer, Object> idToConstant,
      List<Record> sourceRecords,
      BiFunction<Integer, Record, Record> transform)
      throws IOException {
    readAndAssertMetadataColumn(
        fileFormat,
        projectionSchema,
        idToConstant,
        IntStream.range(0, sourceRecords.size())
            .mapToObj(index -> transform.apply(index, sourceRecords.get(index)))
            .toList());
  }

  private void readAndAssertMetadataColumn(
      FileFormat fileFormat,
      Schema projectionSchema,
      Map<Integer, Object> idToConstant,
      List<Record> expectedRecords)
      throws IOException {

    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<T> readRecords;

    var readerBuilder =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(projectionSchema);

    if (idToConstant != null) {
      readerBuilder.idToConstant(convertConstantsToEngine(projectionSchema, idToConstant));
    }

    try (CloseableIterable<T> reader = readerBuilder.build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertThat(readRecords).hasSize(expectedRecords.size());
    assertEquals(
        projectionSchema, convertToEngineRecords(expectedRecords, projectionSchema), readRecords);
  }

  private static Record copy(Record source, Schema sourceSchema, Schema targetSchema) {
    Record result = GenericRecord.create(targetSchema);
    for (Types.NestedField col : sourceSchema.columns()) {
      result.setField(col.name(), source.getField(col.name()));
    }

    return result;
  }
}

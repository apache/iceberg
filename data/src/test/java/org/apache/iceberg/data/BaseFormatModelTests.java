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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.AvroTestHelpers;
import org.apache.iceberg.data.orc.GenericOrcWriter;
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
import org.apache.iceberg.formats.FileWriterBuilder;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.orc.OrcRowWriter;
import org.apache.iceberg.orc.TestORCSchemaUtil;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;

public abstract class BaseFormatModelTests<T> {

  protected abstract Class<T> engineType();

  protected abstract Object engineSchema(Schema schema);

  protected abstract T convertToEngine(Record record, Schema schema);

  protected abstract void assertEquals(Schema schema, List<T> expected, List<T> actual);

  protected boolean supportsBatchReads() {
    return false;
  }

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
  static final String FEATURE_REUSE_CONTAINERS = "reuseContainers";

  private static final Map<FileFormat, String[]> MISSING_FEATURES =
      Map.of(
          FileFormat.AVRO,
          new String[] {FEATURE_FILTER, FEATURE_CASE_SENSITIVE, FEATURE_SPLIT},
          FileFormat.ORC,
          new String[] {FEATURE_REUSE_CONTAINERS});

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

    List<Record> projectedGenericRecords = projectRecords(genericRecords, projectedSchema);
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

  /**
   * Schema evolution: Adding column (reading with wider schema). Write with DefaultSchema, read
   * with additional optional columns. The new columns should be filled with null values.
   */
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testSchemaEvolutionAddColumn(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema writeSchema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, writeSchema, genericRecords);

    List<Types.NestedField> evolvedColumns = Lists.newArrayList(writeSchema.columns());

    int maxFieldId =
        writeSchema.columns().stream().mapToInt(Types.NestedField::fieldId).max().orElse(0);
    evolvedColumns.add(
        Types.NestedField.optional("new_string_col")
            .withId(maxFieldId + 1)
            .ofType(Types.StringType.get())
            .build());
    evolvedColumns.add(
        Types.NestedField.optional("new_int_col")
            .withId(maxFieldId + 2)
            .ofType(Types.IntegerType.get())
            .build());
    Schema readSchema = new Schema(evolvedColumns);
    readAndAssertEngineRecords(
        fileFormat,
        readSchema,
        genericRecords,
        record -> {
          Record expected = GenericRecord.create(readSchema);
          for (Types.NestedField col : writeSchema.columns()) {
            expected.setField(col.name(), record.getField(col.name()));
          }

          expected.setField("new_string_col", null);
          expected.setField("new_int_col", null);
          return expected;
        });
  }

  /**
   * Schema evolution: Projection / Removing column (reading with narrower schema). Write with
   * DefaultSchema, read with only a subset of columns (skipping middle columns).
   */
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testSchemaEvolutionProjection(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema writeSchema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, writeSchema, genericRecords);

    List<Types.NestedField> writeColumns = writeSchema.columns();
    assumeThat(writeColumns).hasSizeGreaterThanOrEqualTo(2);
    Schema projectedSchema =
        new Schema(writeColumns.get(0), writeColumns.get(writeColumns.size() - 1));

    readAndAssertEngineRecords(
        fileFormat,
        projectedSchema,
        genericRecords,
        record -> {
          Record expected = GenericRecord.create(projectedSchema);
          for (Types.NestedField col : projectedSchema.columns()) {
            expected.setField(col.name(), record.getField(col.name()));
          }

          return expected;
        });
  }

  /**
   * Schema evolution: Removing and adding a column with the same name (name mapping). Write with
   * DefaultSchema where col_b has field ID 2. Read with a schema where col_b has field ID 6 (a new
   * column).
   */
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testSchemaEvolutionDropAndReAddSameNameColumn(FileFormat fileFormat) throws IOException {

    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema writeSchema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, writeSchema, genericRecords);

    // Remove col_b and add a new col_b with a different field ID
    Schema readSchema =
        new Schema(
            Types.NestedField.required(1, "col_a", Types.StringType.get()),
            Types.NestedField.optional(6, "col_b", Types.IntegerType.get()),
            Types.NestedField.required(3, "col_c", Types.LongType.get()),
            Types.NestedField.required(4, "col_d", Types.FloatType.get()),
            Types.NestedField.required(5, "col_e", Types.DoubleType.get()));

    readAndAssertEngineRecords(
        fileFormat,
        readSchema,
        genericRecords,
        record -> {
          Record expected = GenericRecord.create(readSchema);
          expected.setField("col_a", record.getField("col_a"));
          expected.setField("col_b", null);
          expected.setField("col_c", record.getField("col_c"));
          expected.setField("col_d", record.getField("col_d"));
          expected.setField("col_e", record.getField("col_e"));
          return expected;
        });
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testSchemaEvolutionTypePromotionIntToLong(FileFormat fileFormat) throws IOException {
    runTypePromotionCheck(
        fileFormat,
        Types.IntegerType.get(),
        Types.LongType.get(),
        value -> value == null ? null : ((Integer) value).longValue());
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testSchemaEvolutionTypePromotionFloatToDouble(FileFormat fileFormat) throws IOException {
    runTypePromotionCheck(
        fileFormat,
        Types.FloatType.get(),
        Types.DoubleType.get(),
        value -> value == null ? null : ((Float) value).doubleValue());
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testSchemaEvolutionTypePromotionDecimalPrecision(FileFormat fileFormat) throws IOException {
    runTypePromotionCheck(
        fileFormat,
        Types.DecimalType.of(9, 2),
        Types.DecimalType.of(18, 2),
        java.util.function.Function.identity());
  }

  /**
   * Schema evolution: Reorder columns. Write with DefaultSchema {col_a, col_b, col_c, col_d,
   * col_e}, read with reordered schema {col_e, col_c, col_a, col_d, col_b}.
   */
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testSchemaEvolutionReorderColumns(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema writeSchema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, writeSchema, genericRecords);

    Schema reorderedSchema =
        new Schema(
            Types.NestedField.required(5, "col_e", Types.DoubleType.get()),
            Types.NestedField.required(3, "col_c", Types.LongType.get()),
            Types.NestedField.required(1, "col_a", Types.StringType.get()),
            Types.NestedField.required(4, "col_d", Types.FloatType.get()),
            Types.NestedField.required(2, "col_b", Types.IntegerType.get()));

    readAndAssertEngineRecords(
        fileFormat,
        reorderedSchema,
        genericRecords,
        record -> {
          Record expected = GenericRecord.create(reorderedSchema);
          for (Types.NestedField col : reorderedSchema.columns()) {
            expected.setField(col.name(), record.getField(col.name()));
          }

          return expected;
        });
  }

  /**
   * Schema evolution: Rename column. Write with DefaultSchema where col_b has field ID 2. Read with
   * a schema where the same field ID 2 is renamed to "column_b". Since Iceberg binds by field ID,
   * the renamed column should still read the original data correctly.
   */
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testSchemaEvolutionRenameColumn(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema writeSchema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, writeSchema, genericRecords);

    // rename col_b(id=2) -> column_b, col_d(id=4) -> column_d
    Schema renamedSchema =
        new Schema(
            Types.NestedField.required(1, "col_a", Types.StringType.get()),
            Types.NestedField.required(2, "column_b", Types.IntegerType.get()),
            Types.NestedField.required(3, "col_c", Types.LongType.get()),
            Types.NestedField.required(4, "column_d", Types.FloatType.get()),
            Types.NestedField.required(5, "col_e", Types.DoubleType.get()));

    readAndAssertEngineRecords(
        fileFormat,
        renamedSchema,
        genericRecords,
        record -> {
          Record expected = GenericRecord.create(renamedSchema);
          expected.setField("col_a", record.getField("col_a"));
          expected.setField("column_b", record.getField("col_b"));
          expected.setField("col_c", record.getField("col_c"));
          expected.setField("column_d", record.getField("col_d"));
          expected.setField("col_e", record.getField("col_e"));
          return expected;
        });
  }

  /**
   * Schema evolution: Required → Optional. Write with DefaultSchema where all columns are required.
   * Read with a schema where some columns are changed to optional. Iceberg allows widening required
   * to optional. The data should still be read correctly.
   */
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testSchemaEvolutionRequiredToOptional(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema writeSchema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, writeSchema, genericRecords);

    // change col_b and col_d to optional
    Schema readSchema =
        new Schema(
            Types.NestedField.required(1, "col_a", Types.StringType.get()),
            Types.NestedField.optional(2, "col_b", Types.IntegerType.get()),
            Types.NestedField.required(3, "col_c", Types.LongType.get()),
            Types.NestedField.optional(4, "col_d", Types.FloatType.get()),
            Types.NestedField.required(5, "col_e", Types.DoubleType.get()));

    readAndAssertEngineRecords(
        fileFormat,
        readSchema,
        genericRecords,
        record -> {
          Record expected = GenericRecord.create(readSchema);
          for (Types.NestedField col : readSchema.columns()) {
            expected.setField(col.name(), record.getField(col.name()));
          }

          return expected;
        });
  }

  /**
   * Schema evolution: Read with empty projection. Write with DefaultSchema, read with an empty
   * schema (no columns). The reader should return the correct number of rows but with no data
   * columns.
   */
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testSchemaEvolutionEmptyProjection(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema writeSchema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, writeSchema, genericRecords);

    Schema emptySchema = new Schema();

    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<T> readRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(emptySchema)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertThat(readRecords).hasSameSizeAs(genericRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReadFileWithoutFieldIdsUsingNameMapping(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema icebergSchema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords();

    // Write the file WITHOUT Iceberg field IDs (as an external writer would).
    writeRecordsWithoutFieldIds(fileFormat, icebergSchema, genericRecords);

    NameMapping nameMapping = MappingUtil.create(icebergSchema);

    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<T> readRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(icebergSchema)
            .withNameMapping(nameMapping)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertEquals(icebergSchema, convertToEngineRecords(genericRecords, icebergSchema), readRecords);
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

  private void writeGenericRecords(FileFormat fileFormat, Schema schema, List<Record> records)
      throws IOException {
    FileWriterBuilder<DataWriter<Record>, Object> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, Record.class, encryptedFile);

    DataWriter<Record> writer =
        writerBuilder.schema(schema).spec(PartitionSpec.unpartitioned()).build();

    try (writer) {
      records.forEach(writer::write);
    }

    DataFile dataFile = writer.toDataFile();
    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(records.size());
    assertThat(dataFile.format()).isEqualTo(fileFormat);
  }

  private List<Record> projectRecords(List<Record> records, Schema projectedSchema) {
    return records.stream()
        .map(
            record -> {
              Record projected = GenericRecord.create(projectedSchema.asStruct());
              projectedSchema
                  .columns()
                  .forEach(
                      field -> projected.setField(field.name(), record.getField(field.name())));
              return projected;
            })
        .toList();
  }

  private List<T> convertToEngineRecords(List<Record> records, Schema schema) {
    return records.stream().map(r -> convertToEngine(r, schema)).toList();
  }

  private static void assumeSupports(FileFormat fileFormat, String feature) {
    assumeThat(MISSING_FEATURES.getOrDefault(fileFormat, new String[] {})).doesNotContain(feature);
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

  private void writeRecordsWithoutFieldIds(
      FileFormat fileFormat, Schema schema, List<Record> records) throws IOException {
    switch (fileFormat) {
      case PARQUET -> writeParquetWithoutFieldIds(schema, records);
      case AVRO -> writeAvroWithoutFieldIds(schema, records);
      case ORC -> writeOrcWithoutFieldIds(schema, records);
      default -> throw new UnsupportedOperationException("Unsupported file format: " + fileFormat);
    }
  }

  private void writeAvroWithoutFieldIds(Schema schema, List<Record> records) throws IOException {
    org.apache.avro.Schema avroSchemaWithoutIds = AvroTestHelpers.removeIds(schema);

    OutputFile outputFile = encryptedFile.encryptingOutputFile();
    DatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>(avroSchemaWithoutIds);
    try (OutputStream out = outputFile.create();
        DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(avroSchemaWithoutIds, out);
      for (Record record : records) {
        GenericData.Record avroRecord = new GenericData.Record(avroSchemaWithoutIds);
        for (Types.NestedField field : schema.columns()) {
          avroRecord.put(field.name(), record.getField(field.name()));
        }

        writer.append(avroRecord);
      }
    }

    try (DataFileStream<GenericData.Record> reader =
        new DataFileStream<>(outputFile.toInputFile().newStream(), new GenericDatumReader<>())) {
      assertThat(AvroTestHelpers.hasIds(reader.getSchema())).isFalse();
    }
  }

  private void writeParquetWithoutFieldIds(Schema schema, List<Record> records) throws IOException {
    org.apache.avro.Schema avroSchemaWithoutIds = AvroTestHelpers.removeIds(schema);

    Path tempFile = Files.createTempFile("iceberg-test-no-ids-", ".parquet");
    Files.deleteIfExists(tempFile);

    try {
      try (ParquetWriter<GenericData.Record> writer =
          AvroParquetWriter.<GenericData.Record>builder(
                  new org.apache.hadoop.fs.Path(tempFile.toUri()))
              .withDataModel(GenericData.get())
              .withSchema(avroSchemaWithoutIds)
              .withConf(new Configuration())
              .build()) {
        for (Record record : records) {
          GenericData.Record avroRecord = new GenericData.Record(avroSchemaWithoutIds);
          for (Types.NestedField field : schema.columns()) {
            avroRecord.put(field.name(), record.getField(field.name()));
          }

          writer.write(avroRecord);
        }
      }

      try (ParquetFileReader reader =
          ParquetFileReader.open(
              HadoopInputFile.fromPath(
                  new org.apache.hadoop.fs.Path(tempFile.toUri()), new Configuration()))) {
        assertThat(ParquetSchemaUtil.hasIds(reader.getFooter().getFileMetaData().getSchema()))
            .isFalse();
      }

      byte[] bytes = Files.readAllBytes(tempFile);
      try (OutputStream out = encryptedFile.encryptingOutputFile().create()) {
        out.write(bytes);
      }
    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  private void writeOrcWithoutFieldIds(Schema schema, List<Record> records) throws IOException {
    TypeDescription typeWithIds = ORCSchemaUtil.convert(schema);
    TypeDescription typeWithoutIds = TestORCSchemaUtil.removeIds(typeWithIds);

    Configuration conf = new Configuration();
    OrcFile.WriterOptions options =
        OrcFile.writerOptions(conf).useUTCTimestamp(true).setSchema(typeWithoutIds);

    OrcRowWriter<Record> rowWriter = GenericOrcWriter.buildWriter(schema, typeWithIds);

    Path tempFile = Files.createTempFile("iceberg-test-no-ids-", ".orc");
    Files.deleteIfExists(tempFile);
    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(tempFile.toUri());

    try {
      try (Writer orcWriter = OrcFile.createWriter(hadoopPath, options)) {
        VectorizedRowBatch batch = typeWithoutIds.createRowBatch();
        for (Record record : records) {
          rowWriter.write(record, batch);
          if (batch.size == batch.getMaxSize()) {
            orcWriter.addRowBatch(batch);
            batch.reset();
          }
        }

        if (batch.size > 0) {
          orcWriter.addRowBatch(batch);
          batch.reset();
        }
      }

      try (Reader reader = OrcFile.createReader(hadoopPath, OrcFile.readerOptions(conf))) {
        assertThat(TestORCSchemaUtil.hasIds(reader.getSchema())).isFalse();
      }

      byte[] bytes = Files.readAllBytes(tempFile);
      try (OutputStream out = encryptedFile.encryptingOutputFile().create()) {
        out.write(bytes);
      }
    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  private void runTypePromotionCheck(
      FileFormat fileFormat, Type fromType, Type toType, Function<Object, Object> promoteValue)
      throws IOException {
    String columnName = "col";
    Schema writeSchema = new Schema(Types.NestedField.required(1, columnName, fromType));
    Schema readSchema = new Schema(Types.NestedField.required(1, columnName, toType));

    List<Record> genericRecords = RandomGenericData.generate(writeSchema, 10, 1L);
    writeGenericRecords(fileFormat, writeSchema, genericRecords);

    readAndAssertEngineRecords(
        fileFormat,
        readSchema,
        genericRecords,
        record -> {
          Record expected = GenericRecord.create(readSchema);
          expected.setField(columnName, promoteValue.apply(record.getField(columnName)));
          return expected;
        });
  }

  private void readAndAssertEngineRecords(
      FileFormat fileFormat,
      Schema readSchema,
      List<Record> sourceRecords,
      Function<Record, Record> converter)
      throws IOException {
    List<Record> expectedGenericRecords = sourceRecords.stream().map(converter).toList();
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<T> readRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(readSchema)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertThat(readRecords).hasSize(expectedGenericRecords.size());
    assertEquals(
        readSchema, convertToEngineRecords(expectedGenericRecords, readSchema), readRecords);
  }
}

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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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

  private static final FileFormat[] FILE_FORMATS =
      new FileFormat[] {FileFormat.AVRO, FileFormat.PARQUET, FileFormat.ORC};

  private static final List<Arguments> FORMAT_AND_GENERATOR =
      Arrays.stream(FILE_FORMATS)
          .flatMap(
              format ->
                  Arrays.stream(DataGenerators.ALL)
                      .map(generator -> Arguments.of(format, generator)))
          .toList();

  private static final ImmutableMap<FileFormat, String[]> MISSING_FEATURES =
      ImmutableMap.of(
          FileFormat.AVRO, new String[] {"filter", "caseSensitive", "recordsPerBatch", "split"},
          FileFormat.ORC, new String[] {"reuseContainers"});

  @TempDir protected Path temp;

  private InMemoryFileIO fileIO;
  private EncryptedOutputFile encryptedFile;

  @BeforeEach
  void before() {
    this.fileIO = new InMemoryFileIO();
    this.encryptedFile =
        EncryptedFiles.encryptedOutput(
            fileIO.newOutputFile("test-file"), EncryptionKeyMetadata.EMPTY);
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

    DataWriter<T> writer =
        writerBuilder
            .schema(schema)
            .engineSchema(engineSchema(schema))
            .spec(PartitionSpec.unpartitioned())
            .build();

    List<Record> genericRecords = dataGenerator.generateRecords();
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);

    try (writer) {
      for (T record : engineRecords) {
        writer.write(record);
      }
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
      for (T record : engineRecords) {
        writer.write(record);
      }
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
            .engineSchema(engineSchema(schema))
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(1)
            .build();

    List<Record> genericRecords = dataGenerator.generateRecords();
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);

    try (writer) {
      for (T record : engineRecords) {
        writer.write(record);
      }
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
      for (T record : engineRecords) {
        writer.write(record);
      }
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
      for (Record record : genericRecords) {
        writer.write(record);
      }
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
      for (PositionDelete<T> delete : deletes) {
        writer.write(delete);
      }
    }

    DeleteFile deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(2);
    assertThat(deleteFile.format()).isEqualTo(fileFormat);

    readAndAssertGenericRecords(fileFormat, positionDeleteSchema, records);
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

  /** Write with Generic Record, read with projected engine type T (narrow schema) */
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReaderBuilderProjection(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema fullSchema = dataGenerator.schema();

    List<Types.NestedField> columns = fullSchema.columns();
    List<Types.NestedField> projectedColumns =
        IntStream.range(0, columns.size())
            .filter(i -> i % 2 == 1)
            .mapToObj(columns::get)
            .collect(Collectors.toList());
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
            .engineProjection(engineSchema(projectedSchema))
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertEquals(projectedSchema, expectedEngineRecords, readRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReaderBuilderFilter(FileFormat fileFormat) throws IOException {

    assumeSupports(fileFormat, "filter");

    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords(10000);
    writeRecordsForSplit(fileFormat, schema, genericRecords);

    // Construct a filter condition that is smaller than the minimum value to achieve file-level
    // filtering.
    Types.NestedField firstField = schema.columns().get(0);
    Object minValue = minFilterField(firstField, genericRecords);
    Expression lessThanFilter = Expressions.lessThan(firstField.name(), minValue);

    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<T> readRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .engineProjection(engineSchema(schema))
            .filter(lessThanFilter)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertThat(readRecords).isEmpty();

    Object maxFilterField = maxFilterField(firstField, genericRecords);
    Expression greaterThanFilter =
        Expressions.greaterThanOrEqual(firstField.name(), maxFilterField);

    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .engineProjection(engineSchema(schema))
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

    assumeSupports(fileFormat, "caseSensitive");

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
            .engineProjection(engineSchema(schema))
            .filter(upperCaseFilter)
            .caseSensitive(false)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertThat(readRecords).isNotEmpty();

    // caseSensitive=true: upper-cased column name cannot be resolved → must throw.
    assertThatThrownBy(
            () -> {
              try (CloseableIterable<T> reader =
                  FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
                      .project(schema)
                      .engineProjection(engineSchema(schema))
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

    assumeSupports(fileFormat, "split");

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
            .engineProjection(engineSchema(schema))
            .split(firstSplitStart, firstSplitLength)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertThat(readRecords).hasSizeGreaterThan(0).hasSizeLessThan(records.size());

    // split(fileLength, 0): empty range at the end of the file → no records should be returned
    List<T> emptyReadRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .engineProjection(engineSchema(schema))
            .split(fileLength, 0)
            .build()) {
      emptyReadRecords = ImmutableList.copyOf(reader);
    }

    assertThat(emptyReadRecords).isEmpty();

    // split(0, fileLength): full file range → all records should be returned
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .engineProjection(engineSchema(schema))
            .split(0, fileLength)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertThat(readRecords).hasSize(records.size());
  }

  /**
   * Verifies the contract of recordsPerBatch: recordsPerBatch is a hint for vectorized readers. The
   * total number of records returned must be unaffected regardless of the batch size value.
   */
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReaderBuilderRecordsPerBatch(FileFormat fileFormat) throws IOException {

    assumeSupports(fileFormat, "recordsPerBatch");

    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords();
    writeGenericRecords(fileFormat, schema, genericRecords);

    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<T> expectedEngineRecords = convertToEngineRecords(genericRecords, schema);

    List<T> smallBatchRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .engineProjection(engineSchema(schema))
            .recordsPerBatch(1)
            .build()) {
      smallBatchRecords = ImmutableList.copyOf(reader);
    }

    assertEquals(schema, expectedEngineRecords, smallBatchRecords);

    List<T> largeBatchRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .engineProjection(engineSchema(schema))
            .recordsPerBatch(genericRecords.size() + 1)
            .build()) {
      largeBatchRecords = ImmutableList.copyOf(reader);
    }

    assertEquals(schema, expectedEngineRecords, largeBatchRecords);
  }

  /** Verifies the contract of reuseContainers */
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testReaderBuilderReuseContainers(FileFormat fileFormat) throws IOException {

    assumeSupports(fileFormat, "reuseContainers");

    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();
    List<Record> genericRecords = dataGenerator.generateRecords();
    // Need at least 2 records to verify container reuse
    assumeThat(genericRecords.size() >= 2).isTrue();
    writeGenericRecords(fileFormat, schema, genericRecords);

    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();

    // Without reuseContainers: every record must be a distinct object instance
    List<T> noReuseRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .engineProjection(engineSchema(schema))
            .build()) {
      noReuseRecords = ImmutableList.copyOf(reader);
    }

    for (int i = 0; i < noReuseRecords.size() - 1; i++) {
      assertThat(noReuseRecords.get(i)).isNotSameAs(noReuseRecords.get(i + 1));
    }

    // With reuseContainers: all collected elements must be the same object instance
    List<T> reuseRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .engineProjection(engineSchema(schema))
            .reuseContainers()
            .build()) {
      reuseRecords = ImmutableList.copyOf(reader);
    }

    T first = reuseRecords.get(0);
    for (int i = 1; i < reuseRecords.size(); i++) {
      assertThat(reuseRecords.get(i)).isSameAs(first);
    }
  }

  private void writeGenericRecords(FileFormat fileFormat, Schema schema, List<Record> records)
      throws IOException {
    FileWriterBuilder<DataWriter<Record>, Object> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, Record.class, encryptedFile);

    DataWriter<Record> writer =
        writerBuilder.schema(schema).spec(PartitionSpec.unpartitioned()).build();

    try (writer) {
      for (Record record : records) {
        writer.write(record);
      }
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

  private Object minFilterField(Types.NestedField firstField, List<Record> records) {
    Object minValue =
        records.stream()
            .map(r -> (Comparable<Object>) r.getField(firstField.name()))
            .min(Comparator.naturalOrder())
            .orElseThrow();
    return minValue;
  }

  private Object maxFilterField(Types.NestedField firstField, List<Record> records) {
    Object minValue =
        records.stream()
            .map(r -> (Comparable<Object>) r.getField(firstField.name()))
            .max(Comparator.naturalOrder())
            .orElseThrow();
    return minValue;
  }

  private List<T> convertToEngineRecords(List<Record> records, Schema schema) {
    return records.stream().map(r -> convertToEngine(r, schema)).toList();
  }

  private static void assumeSupports(FileFormat fileFormat, String feature) {
    String[] missing = MISSING_FEATURES.get(fileFormat);
    if (missing != null) {
      assumeThat(Arrays.stream(missing).noneMatch(f -> f.equals(feature))).isTrue();
    }
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
      for (Record record : records) {
        writer.write(record);
      }
    }

    DataFile dataFile = writer.toDataFile();
    List<Long> splitOffsets = dataFile.splitOffsets();
    assertThat(splitOffsets).hasSizeGreaterThan(1);

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
}

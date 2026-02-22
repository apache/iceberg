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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.parquet.Parquet.writeData;
import static org.apache.iceberg.parquet.ParquetWritingTestUtils.createTempFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestParquetFileMerger {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final long DEFAULT_ROW_GROUP_SIZE =
      TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT;

  @TempDir private Path temp;

  private FileIO fileIO;

  @BeforeEach
  public void setupFileIO() {
    this.fileIO = new TestTables.LocalFileIO();
  }

  @Test
  void canMergeThrowsForEmptyList() {
    assertThatThrownBy(() -> ParquetFileMerger.canMergeAndGetSchema(Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("inputFiles cannot be null or empty");
  }

  @Test
  void canMergeThrowsForNullInput() {
    assertThatThrownBy(() -> ParquetFileMerger.canMergeAndGetSchema(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("inputFiles cannot be null or empty");
  }

  @Test
  void canMergeReturnsNullForNonParquetFile() throws IOException {
    // Create a non-Parquet file (just a text file)
    File textFile = createTempFile(temp);
    textFile.getParentFile().mkdirs(); // Ensure directory exists
    java.nio.file.Files.write(textFile.toPath(), "This is not a Parquet file".getBytes());

    InputFile inputFile = Files.localInput(textFile);
    List<InputFile> inputFiles = Lists.newArrayList(inputFile);

    // Should return null because file is not valid Parquet
    MessageType result = ParquetFileMerger.canMergeAndGetSchema(inputFiles);
    assertThat(result).isNull();
  }

  @Test
  void canMergeReturnsNullForDifferentSchemas() throws IOException {
    // Create first Parquet file with schema1
    Schema icebergSchema1 = SCHEMA;

    File parquetFile1 = createTempFile(temp);
    OutputFile outputFile1 = Files.localOutput(parquetFile1);
    createParquetFileWithData(outputFile1, icebergSchema1, Collections.emptyList());

    // Create second Parquet file with different schema
    Schema icebergSchema2 =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(3, "other", Types.LongType.get())); // Different field

    File parquetFile2 = createTempFile(temp);
    OutputFile outputFile2 = Files.localOutput(parquetFile2);
    createParquetFileWithData(outputFile2, icebergSchema2, Collections.emptyList());

    // Try to validate - should return null due to different schemas
    InputFile inputFile1 = Files.localInput(parquetFile1);
    InputFile inputFile2 = Files.localInput(parquetFile2);
    List<InputFile> inputFiles = Arrays.asList(inputFile1, inputFile2);

    MessageType result = ParquetFileMerger.canMergeAndGetSchema(inputFiles);
    assertThat(result).isNull();
  }

  @Test
  void canMergeReturnsTrueForIdenticalSchemas() throws IOException {
    // Create two Parquet files with the same schema and some data
    File parquetFile1 = createTempFile(temp);
    writeRecordsToFile(parquetFile1, Arrays.asList(createRecord(1, "a")));

    File parquetFile2 = createTempFile(temp);
    writeRecordsToFile(parquetFile2, Arrays.asList(createRecord(2, "b")));

    // Should return non-null MessageType for identical schemas
    InputFile inputFile1 = Files.localInput(parquetFile1);
    InputFile inputFile2 = Files.localInput(parquetFile2);
    List<InputFile> inputFiles = Arrays.asList(inputFile1, inputFile2);

    MessageType result = ParquetFileMerger.canMergeAndGetSchema(inputFiles);
    assertThat(result).isNotNull();
  }

  /** Test that validation catches files with null values in physical row lineage columns. */
  @Test
  void canMergeReturnsNullForPhysicalRowLineageWithNulls() throws IOException {
    Schema schemaWithLineage =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            // OPTIONAL - allows nulls
            Types.NestedField.optional(
                MetadataColumns.ROW_ID.fieldId(),
                MetadataColumns.ROW_ID.name(),
                Types.LongType.get()),
            Types.NestedField.required(
                MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(),
                MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(),
                Types.LongType.get()));

    File file1 = createTempFile(temp);
    // Create file with null in _row_id column (this simulates corrupted data)
    createParquetFileWithData(
        Files.localOutput(file1),
        schemaWithLineage,
        Arrays.asList(
            createRecordWithLineage(schemaWithLineage, 1, "a", null, 5L), // null _row_id
            createRecordWithLineage(schemaWithLineage, 2, "b", 101L, 5L)));

    List<InputFile> inputFiles = Arrays.asList(Files.localInput(file1));

    // canMergeAndGetSchema should return null due to null values
    MessageType result = ParquetFileMerger.canMergeAndGetSchema(inputFiles);
    assertThat(result)
        .as("canMergeAndGetSchema should return null for files with null row lineage values")
        .isNull();
  }

  /** Test schema validation edge case: same field name but different types. */
  @Test
  void canMergeReturnsNullForSameFieldNameDifferentType() throws IOException {
    // Create first file with 'data' as StringType
    Schema schema1 =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    File file1 = createTempFile(temp);
    createParquetFileWithData(Files.localOutput(file1), schema1, Collections.emptyList());

    // Create second file with 'data' as LongType (incompatible)
    Schema schema2 =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.LongType.get()));

    File file2 = createTempFile(temp);
    createParquetFileWithData(Files.localOutput(file2), schema2, Collections.emptyList());

    // Validation should fail due to type mismatch
    List<InputFile> inputFiles = Arrays.asList(Files.localInput(file1), Files.localInput(file2));

    MessageType result = ParquetFileMerger.canMergeAndGetSchema(inputFiles);
    assertThat(result)
        .as("canMergeAndGetSchema should return null for same field name with different types")
        .isNull();
  }

  /**
   * Test that files with different compression codecs can still be merged (compression is at the
   * row group level, not schema level).
   */
  @Test
  void canMergeReturnsTrueForDifferentCompressionCodecs() throws IOException {
    // Create file 1 with SNAPPY compression
    File file1 = createTempFile(temp);
    createParquetFileWithData(
        Files.localOutput(file1),
        SCHEMA,
        Arrays.asList(createRecord(1, "a")),
        TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT,
        "snappy");

    // Create file 2 with GZIP compression
    File file2 = createTempFile(temp);
    createParquetFileWithData(
        Files.localOutput(file2),
        SCHEMA,
        Arrays.asList(createRecord(2, "b")),
        TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT,
        "gzip");

    // Should succeed - compression doesn't affect schema compatibility
    List<InputFile> inputFiles = Arrays.asList(Files.localInput(file1), Files.localInput(file2));

    MessageType result = ParquetFileMerger.canMergeAndGetSchema(inputFiles);
    assertThat(result)
        .as("Files with different compression codecs should be mergeable")
        .isNotNull();
  }

  /** Test error handling for corrupted Parquet files. */
  @Test
  void canMergeReturnsNullForCorruptedParquetFile() throws IOException {
    // Create a valid Parquet file
    File validFile = createTempFile(temp);
    writeRecordsToFile(validFile, Arrays.asList(createRecord(1, "a")));

    // Create a corrupted file by writing partial/invalid Parquet data
    File corruptedFile = createTempFile(temp);
    corruptedFile.getParentFile().mkdirs();
    java.nio.file.Files.write(
        corruptedFile.toPath(), "PAR1InvalidData".getBytes()); // Invalid Parquet content

    // Validation should fail gracefully for corrupted file
    List<InputFile> inputFiles =
        Arrays.asList(Files.localInput(validFile), Files.localInput(corruptedFile));

    MessageType result = ParquetFileMerger.canMergeAndGetSchema(inputFiles);
    assertThat(result).as("canMergeAndGetSchema should return null for corrupted files").isNull();
  }

  /** Test error handling for non-existent file paths. */
  @Test
  void canMergeReturnsNullForNonExistentFile() throws IOException {
    // Create a valid file
    File validFile = createTempFile(temp);
    writeRecordsToFile(validFile, Arrays.asList(createRecord(1, "a")));

    // Reference a non-existent file
    File nonExistentFile = new File(temp.toFile(), "non_existent_file.parquet");

    List<InputFile> inputFiles =
        Arrays.asList(Files.localInput(validFile), Files.localInput(nonExistentFile));

    // Should return null because non-existent file cannot be read
    MessageType result = ParquetFileMerger.canMergeAndGetSchema(inputFiles);
    assertThat(result)
        .as("canMergeAndGetSchema should return null when file doesn't exist")
        .isNull();
  }

  /**
   * Test that merging files with virtual row lineage synthesizes physical _row_id and
   * _last_updated_sequence_number columns.
   */
  @Test
  void mergeFilesSynthesizesRowLineageColumns() throws IOException {
    // Create two files with test data
    File file1 = createTempFile(temp);
    writeRecordsToFile(
        file1, Arrays.asList(createRecord(1, "a"), createRecord(2, "b"), createRecord(3, "c")));

    File file2 = createTempFile(temp);
    writeRecordsToFile(file2, Arrays.asList(createRecord(4, "d"), createRecord(5, "e")));

    // Create DataFiles with row lineage metadata
    List<DataFile> dataFiles =
        Arrays.asList(
            createDataFile(file1.getAbsolutePath(), 100L, 5L),
            createDataFile(file2.getAbsolutePath(), 103L, 5L));

    File mergedFile = createTempFile(temp);
    OutputFile mergedOutput = Files.localOutput(mergedFile);

    // Perform merge
    mergeFilesHelper(
        dataFiles, mergedOutput, DEFAULT_ROW_GROUP_SIZE, PartitionSpec.unpartitioned(), null);

    // Verify the merged file has both row lineage columns
    InputFile mergedInput = Files.localInput(mergedFile);
    MessageType mergedSchema;
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(mergedInput))) {
      mergedSchema = reader.getFooter().getFileMetaData().getSchema();
    }

    assertThat(mergedSchema.containsField(MetadataColumns.ROW_ID.name()))
        .as("Merged file should have _row_id column")
        .isTrue();
    assertThat(mergedSchema.containsField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()))
        .as("Merged file should have _last_updated_sequence_number column")
        .isTrue();

    // Verify row lineage values are correct
    List<RowLineageRecord> records = readRowLineageData(mergedInput);
    assertThat(records).hasSize(5);

    // Verify _row_id values are sequential starting from firstRowId
    // File1 records: 100, 101, 102
    assertThat(records.get(0).rowId).isEqualTo(100L);
    assertThat(records.get(1).rowId).isEqualTo(101L);
    assertThat(records.get(2).rowId).isEqualTo(102L);

    // File2 records: 103, 104
    assertThat(records.get(3).rowId).isEqualTo(103L);
    assertThat(records.get(4).rowId).isEqualTo(104L);

    // Verify _last_updated_sequence_number is constant (5L) for all records
    for (RowLineageRecord record : records) {
      assertThat(record.seqNum).isEqualTo(5L);
    }

    // Verify original data is preserved
    assertThat(records.get(0).id).isEqualTo(1);
    assertThat(records.get(0).data).isEqualTo("a");
    assertThat(records.get(1).id).isEqualTo(2);
    assertThat(records.get(4).data).isEqualTo("e");
  }

  /** Test row lineage synthesis works correctly across multiple row groups. */
  @Test
  void mergeFilesWithMultipleRowGroups() throws IOException {
    // Create file with multiple row groups by setting small row group size
    File file1 = createTempFile(temp);
    OutputFile output1 = Files.localOutput(file1);

    // Create enough records to span multiple row groups
    List<Record> records = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      records.add(createRecord(i, "data" + i));
    }

    createParquetFileWithData(output1, SCHEMA, records, 1024); // Small row group size

    // Merge with row lineage
    List<DataFile> dataFiles = Arrays.asList(createDataFile(file1.getAbsolutePath(), 1000L, 7L));

    File mergedFile = createTempFile(temp);
    OutputFile mergedOutput = Files.localOutput(mergedFile);

    mergeFilesHelper(
        dataFiles, mergedOutput, DEFAULT_ROW_GROUP_SIZE, PartitionSpec.unpartitioned(), null);

    // Verify row IDs are sequential across row groups
    List<RowLineageRecord> mergedRecords = readRowLineageData(Files.localInput(mergedFile));
    assertThat(mergedRecords).hasSize(100);

    for (int i = 0; i < 100; i++) {
      assertThat(mergedRecords.get(i).rowId).isEqualTo(1000L + i);
      assertThat(mergedRecords.get(i).seqNum).isEqualTo(7L);
    }
  }

  /** Test that each file's dataSequenceNumber is correctly applied to its rows. */
  @Test
  void mergeFilesWithDifferentDataSequenceNumbers() throws IOException {
    // Create three files
    File file1 = createTempFile(temp);
    writeRecordsToFile(file1, Arrays.asList(createRecord(1, "a"), createRecord(2, "b")));

    File file2 = createTempFile(temp);
    writeRecordsToFile(file2, Arrays.asList(createRecord(3, "c")));

    File file3 = createTempFile(temp);
    writeRecordsToFile(
        file3, Arrays.asList(createRecord(4, "d"), createRecord(5, "e"), createRecord(6, "f")));

    // Merge with different sequence numbers for each file
    List<DataFile> dataFiles =
        Arrays.asList(
            createDataFile(file1.getAbsolutePath(), 0L, 10L),
            createDataFile(file2.getAbsolutePath(), 2L, 20L),
            createDataFile(file3.getAbsolutePath(), 3L, 30L));

    File mergedFile = createTempFile(temp);

    mergeFilesHelper(
        dataFiles,
        Files.localOutput(mergedFile),
        DEFAULT_ROW_GROUP_SIZE,
        PartitionSpec.unpartitioned(),
        null);

    // Verify sequence numbers transition correctly between files
    List<RowLineageRecord> records = readRowLineageData(Files.localInput(mergedFile));
    assertThat(records).hasSize(6);

    // File1 records (2 records, seqNum=10)
    assertThat(records.get(0).seqNum).isEqualTo(10L);
    assertThat(records.get(1).seqNum).isEqualTo(10L);

    // File2 records (1 record, seqNum=20)
    assertThat(records.get(2).seqNum).isEqualTo(20L);

    // File3 records (3 records, seqNum=30)
    assertThat(records.get(3).seqNum).isEqualTo(30L);
    assertThat(records.get(4).seqNum).isEqualTo(30L);
    assertThat(records.get(5).seqNum).isEqualTo(30L);
  }

  /** Test that merging without firstRowIds/dataSequenceNumbers works (no row lineage columns). */
  @Test
  void mergeFilesWithoutRowLineage() throws IOException {
    File file1 = createTempFile(temp);
    writeRecordsToFile(file1, Arrays.asList(createRecord(1, "a"), createRecord(2, "b")));

    File file2 = createTempFile(temp);
    writeRecordsToFile(file2, Arrays.asList(createRecord(3, "c")));

    // Merge without row lineage (null firstRowIds and dataSequenceNumbers)
    List<DataFile> dataFiles =
        Arrays.asList(
            createDataFile(file1.getAbsolutePath(), null, null),
            createDataFile(file2.getAbsolutePath(), null, null));

    File mergedFile = createTempFile(temp);

    mergeFilesHelper(
        dataFiles,
        Files.localOutput(mergedFile),
        DEFAULT_ROW_GROUP_SIZE,
        PartitionSpec.unpartitioned(),
        null);

    // Verify merged file does NOT have row lineage columns
    MessageType mergedSchema;
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(mergedFile)))) {
      mergedSchema = reader.getFooter().getFileMetaData().getSchema();
    }

    assertThat(mergedSchema.containsField(MetadataColumns.ROW_ID.name()))
        .as("Merged file should not have _row_id column when no lineage provided")
        .isFalse();
    assertThat(mergedSchema.containsField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()))
        .as(
            "Merged file should not have _last_updated_sequence_number column when no lineage provided")
        .isFalse();

    // Verify data is still correct
    List<Record> records = readData(Files.localInput(mergedFile), SCHEMA);
    assertThat(records).hasSize(3);
    assertThat(records.get(0).getField("id")).isEqualTo(1);
    assertThat(records.get(1).getField("id")).isEqualTo(2);
    assertThat(records.get(2).getField("id")).isEqualTo(3);
  }

  /** Test that files already having physical row lineage columns are preserved via binary copy. */
  @Test
  void mergeFilesWithPhysicalRowLineageColumns() throws IOException {
    // Create schema with row lineage columns
    Schema schemaWithLineage =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.required(
                MetadataColumns.ROW_ID.fieldId(),
                MetadataColumns.ROW_ID.name(),
                Types.LongType.get()),
            Types.NestedField.required(
                MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(),
                MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(),
                Types.LongType.get()));

    // Create files with physical row lineage columns
    File file1 = createTempFile(temp);
    createParquetFileWithData(
        Files.localOutput(file1),
        schemaWithLineage,
        Arrays.asList(
            createRecordWithLineage(schemaWithLineage, 1, "a", 100L, 5L),
            createRecordWithLineage(schemaWithLineage, 2, "b", 101L, 5L)));

    File file2 = createTempFile(temp);
    createParquetFileWithData(
        Files.localOutput(file2),
        schemaWithLineage,
        Arrays.asList(createRecordWithLineage(schemaWithLineage, 3, "c", 102L, 5L)));

    // Merge with firstRowIds/dataSequenceNumbers (should use binary copy path)
    List<DataFile> dataFiles =
        Arrays.asList(
            createDataFile(file1.getAbsolutePath(), 100L, 5L),
            createDataFile(file2.getAbsolutePath(), 102L, 5L));

    File mergedFile = createTempFile(temp);

    mergeFilesHelper(
        dataFiles,
        Files.localOutput(mergedFile),
        DEFAULT_ROW_GROUP_SIZE,
        PartitionSpec.unpartitioned(),
        null);

    // Verify physical columns are preserved
    List<RowLineageRecord> records = readRowLineageData(Files.localInput(mergedFile));
    assertThat(records).hasSize(3);

    // Values should match original files (binary copy preserves them)
    assertThat(records.get(0).rowId).isEqualTo(100L);
    assertThat(records.get(0).seqNum).isEqualTo(5L);
    assertThat(records.get(1).rowId).isEqualTo(101L);
    assertThat(records.get(1).seqNum).isEqualTo(5L);
    assertThat(records.get(2).rowId).isEqualTo(102L);
    assertThat(records.get(2).seqNum).isEqualTo(5L);
  }

  /** Test that verifies exact row ID and sequence number values in merged output. */
  @Test
  void mergeFilesVerifiesExactRowIdValues() throws IOException {
    // Create file 1 with 3 records, firstRowId=100, dataSequenceNumber=5
    File file1 = createTempFile(temp);
    writeRecordsToFile(
        file1, Arrays.asList(createRecord(1, "a"), createRecord(2, "b"), createRecord(3, "c")));

    // Create file 2 with 2 records, firstRowId=200, dataSequenceNumber=7
    File file2 = createTempFile(temp);
    writeRecordsToFile(file2, Arrays.asList(createRecord(4, "d"), createRecord(5, "e")));

    // Create file 3 with 4 records, firstRowId=300, dataSequenceNumber=10
    File file3 = createTempFile(temp);
    writeRecordsToFile(
        file3,
        Arrays.asList(
            createRecord(6, "f"),
            createRecord(7, "g"),
            createRecord(8, "h"),
            createRecord(9, "i")));

    List<DataFile> dataFiles =
        Arrays.asList(
            createDataFile(file1.getAbsolutePath(), 100L, 5L),
            createDataFile(file2.getAbsolutePath(), 200L, 7L),
            createDataFile(file3.getAbsolutePath(), 300L, 10L));

    // Merge files with row lineage preservation
    File outputFile = createTempFile(temp);

    mergeFilesHelper(
        dataFiles,
        Files.localOutput(outputFile),
        DEFAULT_ROW_GROUP_SIZE,
        PartitionSpec.unpartitioned(),
        null);

    // Read back the merged data and verify exact row ID values
    List<RowLineageRecord> records = readRowLineageData(Files.localInput(outputFile));

    // Verify we have all 9 records (3 + 2 + 4)
    assertThat(records).hasSize(9);

    // Verify exact row ID and sequence number values for file 1 (3 records)
    assertThat(records.get(0).rowId).isEqualTo(100L); // firstRowId=100, offset=0
    assertThat(records.get(0).seqNum).isEqualTo(5L);
    assertThat(records.get(1).rowId).isEqualTo(101L); // firstRowId=100, offset=1
    assertThat(records.get(1).seqNum).isEqualTo(5L);
    assertThat(records.get(2).rowId).isEqualTo(102L); // firstRowId=100, offset=2
    assertThat(records.get(2).seqNum).isEqualTo(5L);

    // Verify exact row ID and sequence number values for file 2 (2 records)
    assertThat(records.get(3).rowId).isEqualTo(200L); // firstRowId=200, offset=0
    assertThat(records.get(3).seqNum).isEqualTo(7L);
    assertThat(records.get(4).rowId).isEqualTo(201L); // firstRowId=200, offset=1
    assertThat(records.get(4).seqNum).isEqualTo(7L);

    // Verify exact row ID and sequence number values for file 3 (4 records)
    assertThat(records.get(5).rowId).isEqualTo(300L); // firstRowId=300, offset=0
    assertThat(records.get(5).seqNum).isEqualTo(10L);
    assertThat(records.get(6).rowId).isEqualTo(301L); // firstRowId=300, offset=1
    assertThat(records.get(6).seqNum).isEqualTo(10L);
    assertThat(records.get(7).rowId).isEqualTo(302L); // firstRowId=300, offset=2
    assertThat(records.get(7).seqNum).isEqualTo(10L);
    assertThat(records.get(8).rowId).isEqualTo(303L); // firstRowId=300, offset=3
    assertThat(records.get(8).seqNum).isEqualTo(10L);
  }

  /**
   * Test row lineage synthesis works correctly with partitioned tables. Note: In Iceberg
   * partitioned tables, partition columns are NOT stored in Parquet files, they are encoded in the
   * file path and DataFile metadata.
   */
  @Test
  void mergeFilesWithPartitionedTable() throws IOException {

    // Create schema with partition column
    Schema fullSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "category", Types.StringType.get()));

    // Create partition spec on category
    PartitionSpec spec = PartitionSpec.builderFor(fullSchema).identity("category").build();

    // Create partition value for category=A
    StructLike partitionA =
        GenericRecord.create(spec.partitionType()).copy(Collections.singletonMap("category", "A"));

    // Create two files in the same partition (partition column NOT in file data)
    File file1 = createTempFile(temp);
    writeRecordsToFile(file1, Arrays.asList(createRecord(1, "row1"), createRecord(2, "row2")));

    File file2 = createTempFile(temp);
    writeRecordsToFile(file2, Arrays.asList(createRecord(3, "row3")));

    // Merge with row lineage
    List<DataFile> dataFiles =
        Arrays.asList(
            createDataFile(file1.getAbsolutePath(), 100L, 5L),
            createDataFile(file2.getAbsolutePath(), 102L, 5L));

    File mergedFile = createTempFile(temp);

    mergeFilesHelper(
        dataFiles, Files.localOutput(mergedFile), DEFAULT_ROW_GROUP_SIZE, spec, partitionA);

    // Verify row lineage is correctly synthesized
    List<RowLineageRecord> records = readRowLineageData(Files.localInput(mergedFile));
    assertThat(records).hasSize(3);

    // Verify row IDs are sequential
    assertThat(records.get(0).rowId).isEqualTo(100L);
    assertThat(records.get(1).rowId).isEqualTo(101L);
    assertThat(records.get(2).rowId).isEqualTo(102L);

    // Verify all have same sequence number
    for (RowLineageRecord record : records) {
      assertThat(record.seqNum).isEqualTo(5L);
    }

    // Verify original data is preserved
    assertThat(records.get(0).id).isEqualTo(1);
    assertThat(records.get(1).id).isEqualTo(2);
    assertThat(records.get(2).id).isEqualTo(3);
  }

  /** Helper to create a GenericRecord using the default SCHEMA */
  private Record createRecord(int id, String data) {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  /** Helper to write records to a file using the default SCHEMA */
  private void writeRecordsToFile(File file, List<Record> records) throws IOException {
    createParquetFileWithData(Files.localOutput(file), SCHEMA, records);
  }

  /** Helper to create a GenericRecord with row lineage */
  private Record createRecordWithLineage(
      Schema schema, int id, String data, Long rowId, Long seqNum) {
    Record record = GenericRecord.create(schema);
    record.setField("id", id);
    record.setField("data", data);
    record.setField(MetadataColumns.ROW_ID.name(), rowId);
    record.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), seqNum);
    return record;
  }

  /** Helper to create a DataFile from a file path - wraps it as a GenericDataFile */
  private DataFile createDataFile(String path, Long firstRowId, Long dataSequenceNumber) {
    DataFiles.Builder builder =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(path)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(new File(path).length())
            .withRecordCount(1); // dummy value

    if (firstRowId != null) {
      builder.withFirstRowId(firstRowId);
    }

    DataFile baseFile = builder.build();

    // Use reflection to set dataSequenceNumber since DataFiles.Builder doesn't support it
    try {
      // Access BaseFile.setDataSequenceNumber via reflection so BaseFile can remain
      // package-private and the test does not depend on its visibility.
      Class<?> baseFileClass = Class.forName("org.apache.iceberg.BaseFile");
      java.lang.reflect.Method setDataSeq =
          baseFileClass.getDeclaredMethod("setDataSequenceNumber", Long.class);
      setDataSeq.setAccessible(true);
      setDataSeq.invoke(baseFileClass.cast(baseFile), dataSequenceNumber);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to set dataSequenceNumber on DataFile", e);
    }

    return baseFile;
  }

  /** Helper to create a Parquet file with data */
  private void createParquetFileWithData(OutputFile outputFile, Schema schema, List<Record> records)
      throws IOException {
    createParquetFileWithData(
        outputFile, schema, records, TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT);
  }

  /** Helper to create a Parquet file with data and specified row group size */
  private void createParquetFileWithData(
      OutputFile outputFile, Schema schema, List<Record> records, long rowGroupSize)
      throws IOException {
    createParquetFileWithData(outputFile, schema, records, rowGroupSize, null);
  }

  /** Helper to create a Parquet file with data, row group size, and compression codec */
  private void createParquetFileWithData(
      OutputFile outputFile,
      Schema schema,
      List<Record> records,
      long rowGroupSize,
      String compression)
      throws IOException {
    Parquet.DataWriteBuilder writerBuilder =
        writeData(outputFile)
            .schema(schema)
            .withSpec(PartitionSpec.unpartitioned())
            .createWriterFunc(GenericParquetWriter::create)
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, String.valueOf(rowGroupSize));

    if (compression != null) {
      writerBuilder.set(TableProperties.PARQUET_COMPRESSION, compression);
    }

    DataWriter<Record> writer = writerBuilder.overwrite().build();
    try {
      for (Record record : records) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }
  }

  /** Helper to read data from Parquet file */
  private List<Record> readData(InputFile inputFile, Schema schema) throws IOException {
    List<Record> records = Lists.newArrayList();
    try (CloseableIterable<Record> reader =
        Parquet.read(inputFile)
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      for (Record record : reader) {
        records.add(record);
      }
    }

    return records;
  }

  /** Helper to read data with row lineage from Parquet file using raw Parquet API */
  private List<RowLineageRecord> readRowLineageData(InputFile inputFile) throws IOException {
    List<RowLineageRecord> records = Lists.newArrayList();

    try (ParquetFileReader fileReader = ParquetFileReader.open(ParquetIO.file(inputFile))) {
      MessageType schema = fileReader.getFileMetaData().getSchema();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

      PageReadStore pages;
      while (null != (pages = fileReader.readNextRowGroup())) {
        long rows = pages.getRowCount();
        RowLineageRecordMaterializer materializer = new RowLineageRecordMaterializer();
        RecordReader<RowLineageRecord> recordReader = columnIO.getRecordReader(pages, materializer);

        for (long i = 0; i < rows; i++) {
          records.add(recordReader.read());
        }
      }
    }

    return records;
  }

  /** Materializer for reading row lineage data from Parquet */
  private static class RowLineageRecordMaterializer
      extends org.apache.parquet.io.api.RecordMaterializer<RowLineageRecord> {
    private final RowLineageRootConverter root = new RowLineageRootConverter();

    @Override
    public RowLineageRecord getCurrentRecord() {
      return root.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
      return root;
    }
  }

  /** Root converter for reading all fields into RowLineageRecord */
  private static class RowLineageRootConverter extends GroupConverter {
    private Integer id;
    private String data;
    private Long rowId;
    private Long seqNum;

    private final Converter[] converters =
        new Converter[] {
          new PrimitiveConverter() { // id
            @Override
            public void addInt(int value) {
              id = value;
            }
          },
          new PrimitiveConverter() { // data
            @Override
            public void addBinary(org.apache.parquet.io.api.Binary value) {
              data = value.toStringUsingUTF8();
            }
          },
          new PrimitiveConverter() { // _row_id
            @Override
            public void addLong(long value) {
              rowId = value;
            }
          },
          new PrimitiveConverter() { // _last_updated_sequence_number
            @Override
            public void addLong(long value) {
              seqNum = value;
            }
          }
        };

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    @Override
    public void start() {
      id = null;
      data = null;
      rowId = null;
      seqNum = null;
    }

    @Override
    public void end() {}

    public RowLineageRecord getCurrentRecord() {
      return new RowLineageRecord(rowId, seqNum, id, data);
    }
  }

  /** Helper class to hold row lineage data */
  private static class RowLineageRecord {
    final Long rowId;
    final Long seqNum;
    final Integer id;
    final String data;

    RowLineageRecord(Long rowId, Long seqNum, Integer id, String data) {
      this.rowId = rowId;
      this.seqNum = seqNum;
      this.id = id;
      this.data = data;
    }
  }

  /**
   * Helper method to call mergeFiles with schema validation. This gets the schema first using
   * canMergeAndGetSchema to avoid redundant file reads.
   */
  private DataFile mergeFilesHelper(
      List<DataFile> dataFiles,
      OutputFile outputFile,
      long rowGroupSize,
      PartitionSpec spec,
      StructLike partition)
      throws IOException {
    // Get schema using canMergeAndGetSchema
    MessageType schema =
        ParquetFileMerger.canMergeAndGetSchema(
            dataFiles, fileIO, Long.MAX_VALUE /* no size check */);

    if (schema == null) {
      throw new IllegalArgumentException("Files cannot be merged - validation failed");
    }

    // Call mergeFiles with the schema
    return ParquetFileMerger.mergeFiles(
        dataFiles, fileIO, outputFile, schema, rowGroupSize, spec, partition);
  }
}

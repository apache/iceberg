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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestParquetFileMerger {

  @TempDir private File tempDir;

  @Test
  public void testCanMergeReturnsFalseForEmptyList() {
    boolean result = ParquetFileMerger.canMerge(Collections.emptyList());
    assertThat(result).isFalse();
  }

  @Test
  public void testCanMergeReturnsFalseForNullInput() {
    boolean result = ParquetFileMerger.canMerge(null);
    assertThat(result).isFalse();
  }

  @Test
  public void testCanMergeReturnsFalseForNonParquetFile() throws IOException {
    // Create a non-Parquet file (just a text file)
    File textFile = new File(tempDir, "not-parquet.txt");
    java.nio.file.Files.write(textFile.toPath(), "This is not a Parquet file".getBytes());

    InputFile inputFile = Files.localInput(textFile);
    List<InputFile> inputFiles = Lists.newArrayList(inputFile);

    // Should return false because file is not valid Parquet
    boolean result = ParquetFileMerger.canMerge(inputFiles);
    assertThat(result).isFalse();
  }

  @Test
  public void testCanMergeReturnsFalseForDifferentSchemas() throws IOException {
    // Create first Parquet file with schema1
    Schema icebergSchema1 =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    File parquetFile1 = new File(tempDir, "file1.parquet");
    OutputFile outputFile1 = Files.localOutput(parquetFile1);
    createParquetFileWithSchema(outputFile1, icebergSchema1);

    // Create second Parquet file with different schema
    Schema icebergSchema2 =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(3, "other", Types.LongType.get())); // Different field

    File parquetFile2 = new File(tempDir, "file2.parquet");
    OutputFile outputFile2 = Files.localOutput(parquetFile2);
    createParquetFileWithSchema(outputFile2, icebergSchema2);

    // Try to validate - should return false due to different schemas
    InputFile inputFile1 = Files.localInput(parquetFile1);
    InputFile inputFile2 = Files.localInput(parquetFile2);
    List<InputFile> inputFiles = Arrays.asList(inputFile1, inputFile2);

    boolean result = ParquetFileMerger.canMerge(inputFiles);
    assertThat(result).isFalse();
  }

  @Test
  public void testCanMergeReturnsTrueForIdenticalSchemas() throws IOException {
    // Create two Parquet files with the same schema and some data
    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    File parquetFile1 = new File(tempDir, "file1.parquet");
    createParquetFileWithData(
        Files.localOutput(parquetFile1),
        icebergSchema,
        Arrays.asList(createRecord(icebergSchema, 1, "a")));

    File parquetFile2 = new File(tempDir, "file2.parquet");
    createParquetFileWithData(
        Files.localOutput(parquetFile2),
        icebergSchema,
        Arrays.asList(createRecord(icebergSchema, 2, "b")));

    // Should return true for identical schemas
    InputFile inputFile1 = Files.localInput(parquetFile1);
    InputFile inputFile2 = Files.localInput(parquetFile2);
    List<InputFile> inputFiles = Arrays.asList(inputFile1, inputFile2);

    boolean result = ParquetFileMerger.canMerge(inputFiles);
    assertThat(result).isTrue();
  }

  @Test
  public void testCanMergeReturnsTrueForSingleFile() throws IOException {
    // Create a single Parquet file with some data
    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    File parquetFile = new File(tempDir, "file.parquet");
    createParquetFileWithData(
        Files.localOutput(parquetFile),
        icebergSchema,
        Arrays.asList(createRecord(icebergSchema, 1, "a")));

    // Should return true for single file
    InputFile inputFile = Files.localInput(parquetFile);
    List<InputFile> inputFiles = Lists.newArrayList(inputFile);

    boolean result = ParquetFileMerger.canMerge(inputFiles);
    assertThat(result).isTrue();
  }

  @Test
  public void testMergeFilesSynthesizesRowLineageColumns() throws IOException {
    // Test that merging files with virtual row lineage synthesizes physical _row_id and
    // _last_updated_sequence_number columns
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    // Create two files with test data
    File file1 = new File(tempDir, "file1.parquet");
    OutputFile output1 = Files.localOutput(file1);
    createParquetFileWithData(
        output1,
        schema,
        Arrays.asList(
            createRecord(schema, 1, "a"),
            createRecord(schema, 2, "b"),
            createRecord(schema, 3, "c")));

    File file2 = new File(tempDir, "file2.parquet");
    OutputFile output2 = Files.localOutput(file2);
    createParquetFileWithData(
        output2, schema, Arrays.asList(createRecord(schema, 4, "d"), createRecord(schema, 5, "e")));

    // Merge with row lineage metadata
    List<InputFile> inputFiles = Arrays.asList(Files.localInput(file1), Files.localInput(file2));
    List<Long> firstRowIds = Arrays.asList(100L, 103L); // file1 starts at 100, file2 at 103
    List<Long> dataSequenceNumbers = Arrays.asList(5L, 5L); // both files from sequence 5

    File mergedFile = new File(tempDir, "merged.parquet");
    OutputFile mergedOutput = Files.localOutput(mergedFile);

    // Read schema and metadata from first file
    MessageType parquetSchema = ParquetFileMerger.readSchema(inputFiles.get(0));
    Map<String, String> metadata = ParquetFileMerger.readMetadata(inputFiles.get(0));

    // Perform merge
    ParquetFileMerger.mergeFiles(
        inputFiles,
        EncryptedFiles.plainAsEncryptedOutput(mergedOutput),
        parquetSchema,
        firstRowIds,
        dataSequenceNumbers,
        TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT,
        ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
        metadata);

    // Verify the merged file has both row lineage columns
    InputFile mergedInput = Files.localInput(mergedFile);
    MessageType mergedSchema = ParquetFileMerger.readSchema(mergedInput);
    assertThat(mergedSchema.containsField(MetadataColumns.ROW_ID.name()))
        .as("Merged file should have _row_id column")
        .isTrue();
    assertThat(mergedSchema.containsField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()))
        .as("Merged file should have _last_updated_sequence_number column")
        .isTrue();

    // Verify row lineage values are correct
    List<RowLineageRecord> records = readRowLineageData(mergedInput, schema);
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

  @Test
  public void testMergeFilesWithMultipleRowGroups() throws IOException {
    // Test row lineage synthesis works correctly across multiple row groups
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    // Create file with multiple row groups by setting small row group size
    File file1 = new File(tempDir, "file1.parquet");
    OutputFile output1 = Files.localOutput(file1);

    // Create enough records to span multiple row groups
    List<Record> records = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      records.add(createRecord(schema, i, "data" + i));
    }
    createParquetFileWithData(output1, schema, records, 1024); // Small row group size

    // Merge with row lineage
    List<InputFile> inputFiles = Arrays.asList(Files.localInput(file1));
    List<Long> firstRowIds = Arrays.asList(1000L);
    List<Long> dataSequenceNumbers = Arrays.asList(7L);

    File mergedFile = new File(tempDir, "merged.parquet");
    OutputFile mergedOutput = Files.localOutput(mergedFile);

    MessageType parquetSchema = ParquetFileMerger.readSchema(inputFiles.get(0));
    Map<String, String> metadata = ParquetFileMerger.readMetadata(inputFiles.get(0));

    ParquetFileMerger.mergeFiles(
        inputFiles,
        EncryptedFiles.plainAsEncryptedOutput(mergedOutput),
        parquetSchema,
        firstRowIds,
        dataSequenceNumbers,
        TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT,
        ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
        metadata);

    // Verify row IDs are sequential across row groups
    List<RowLineageRecord> mergedRecords = readRowLineageData(Files.localInput(mergedFile), schema);
    assertThat(mergedRecords).hasSize(100);

    for (int i = 0; i < 100; i++) {
      assertThat(mergedRecords.get(i).rowId).isEqualTo(1000L + i);
      assertThat(mergedRecords.get(i).seqNum).isEqualTo(7L);
    }
  }

  @Test
  public void testMergeFilesWithDifferentDataSequenceNumbers() throws IOException {
    // Test that each file's dataSequenceNumber is correctly applied to its rows
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    // Create three files
    File file1 = new File(tempDir, "file1.parquet");
    createParquetFileWithData(
        Files.localOutput(file1),
        schema,
        Arrays.asList(createRecord(schema, 1, "a"), createRecord(schema, 2, "b")));

    File file2 = new File(tempDir, "file2.parquet");
    createParquetFileWithData(
        Files.localOutput(file2), schema, Arrays.asList(createRecord(schema, 3, "c")));

    File file3 = new File(tempDir, "file3.parquet");
    createParquetFileWithData(
        Files.localOutput(file3),
        schema,
        Arrays.asList(
            createRecord(schema, 4, "d"),
            createRecord(schema, 5, "e"),
            createRecord(schema, 6, "f")));

    // Merge with different sequence numbers for each file
    List<InputFile> inputFiles =
        Arrays.asList(Files.localInput(file1), Files.localInput(file2), Files.localInput(file3));
    List<Long> firstRowIds = Arrays.asList(0L, 2L, 3L);
    List<Long> dataSequenceNumbers = Arrays.asList(10L, 20L, 30L); // Different for each file

    File mergedFile = new File(tempDir, "merged.parquet");
    MessageType parquetSchema = ParquetFileMerger.readSchema(inputFiles.get(0));
    Map<String, String> metadata = ParquetFileMerger.readMetadata(inputFiles.get(0));

    ParquetFileMerger.mergeFiles(
        inputFiles,
        EncryptedFiles.plainAsEncryptedOutput(Files.localOutput(mergedFile)),
        parquetSchema,
        firstRowIds,
        dataSequenceNumbers,
        TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT,
        ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
        metadata);

    // Verify sequence numbers transition correctly between files
    List<RowLineageRecord> records = readRowLineageData(Files.localInput(mergedFile), schema);
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

  @Test
  public void testMergeFilesWithoutRowLineage() throws IOException {
    // Test that merging without firstRowIds/dataSequenceNumbers works (no row lineage columns)
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    File file1 = new File(tempDir, "file1.parquet");
    createParquetFileWithData(
        Files.localOutput(file1),
        schema,
        Arrays.asList(createRecord(schema, 1, "a"), createRecord(schema, 2, "b")));

    File file2 = new File(tempDir, "file2.parquet");
    createParquetFileWithData(
        Files.localOutput(file2), schema, Arrays.asList(createRecord(schema, 3, "c")));

    // Merge without row lineage (null firstRowIds and dataSequenceNumbers)
    List<InputFile> inputFiles = Arrays.asList(Files.localInput(file1), Files.localInput(file2));

    File mergedFile = new File(tempDir, "merged.parquet");
    MessageType parquetSchema = ParquetFileMerger.readSchema(inputFiles.get(0));
    Map<String, String> metadata = ParquetFileMerger.readMetadata(inputFiles.get(0));

    ParquetFileMerger.mergeFiles(
        inputFiles,
        EncryptedFiles.plainAsEncryptedOutput(Files.localOutput(mergedFile)),
        parquetSchema,
        null, // No firstRowIds
        null, // No dataSequenceNumbers
        TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT,
        ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
        metadata);

    // Verify merged file does NOT have row lineage columns
    MessageType mergedSchema = ParquetFileMerger.readSchema(Files.localInput(mergedFile));
    assertThat(mergedSchema.containsField(MetadataColumns.ROW_ID.name()))
        .as("Merged file should not have _row_id column when no lineage provided")
        .isFalse();
    assertThat(mergedSchema.containsField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()))
        .as(
            "Merged file should not have _last_updated_sequence_number column when no lineage provided")
        .isFalse();

    // Verify data is still correct
    List<Record> records = readData(Files.localInput(mergedFile), schema);
    assertThat(records).hasSize(3);
    assertThat(records.get(0).getField("id")).isEqualTo(1);
    assertThat(records.get(1).getField("id")).isEqualTo(2);
    assertThat(records.get(2).getField("id")).isEqualTo(3);
  }

  @Test
  public void testMergeFilesWithPhysicalRowLineageColumns() throws IOException {
    // Test that files already having physical row lineage columns are preserved via binary copy
    Schema baseSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

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
    File file1 = new File(tempDir, "file1.parquet");
    createParquetFileWithRowLineage(
        Files.localOutput(file1),
        schemaWithLineage,
        Arrays.asList(
            createRecordWithLineage(schemaWithLineage, 1, "a", 100L, 5L),
            createRecordWithLineage(schemaWithLineage, 2, "b", 101L, 5L)));

    File file2 = new File(tempDir, "file2.parquet");
    createParquetFileWithRowLineage(
        Files.localOutput(file2),
        schemaWithLineage,
        Arrays.asList(createRecordWithLineage(schemaWithLineage, 3, "c", 102L, 5L)));

    // Merge with firstRowIds/dataSequenceNumbers (should use binary copy path)
    List<InputFile> inputFiles = Arrays.asList(Files.localInput(file1), Files.localInput(file2));
    List<Long> firstRowIds = Arrays.asList(100L, 102L);
    List<Long> dataSequenceNumbers = Arrays.asList(5L, 5L);

    File mergedFile = new File(tempDir, "merged.parquet");
    MessageType parquetSchema = ParquetFileMerger.readSchema(inputFiles.get(0));
    Map<String, String> metadata = ParquetFileMerger.readMetadata(inputFiles.get(0));

    ParquetFileMerger.mergeFiles(
        inputFiles,
        EncryptedFiles.plainAsEncryptedOutput(Files.localOutput(mergedFile)),
        parquetSchema,
        firstRowIds,
        dataSequenceNumbers,
        TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT,
        ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
        metadata);

    // Verify physical columns are preserved
    List<RowLineageRecord> records = readRowLineageData(Files.localInput(mergedFile), baseSchema);
    assertThat(records).hasSize(3);

    // Values should match original files (binary copy preserves them)
    assertThat(records.get(0).rowId).isEqualTo(100L);
    assertThat(records.get(0).seqNum).isEqualTo(5L);
    assertThat(records.get(1).rowId).isEqualTo(101L);
    assertThat(records.get(1).seqNum).isEqualTo(5L);
    assertThat(records.get(2).rowId).isEqualTo(102L);
    assertThat(records.get(2).seqNum).isEqualTo(5L);
  }

  @Test
  public void testCanMergeReturnsFalseForPhysicalRowLineageWithNulls() throws IOException {
    // Test that validation catches files with null values in physical row lineage columns
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

    File file1 = new File(tempDir, "file1.parquet");
    // Create file with null in _row_id column (this simulates corrupted data)
    createParquetFileWithRowLineage(
        Files.localOutput(file1),
        schemaWithLineage,
        Arrays.asList(
            createRecordWithLineage(schemaWithLineage, 1, "a", null, 5L), // null _row_id
            createRecordWithLineage(schemaWithLineage, 2, "b", 101L, 5L)));

    List<InputFile> inputFiles = Arrays.asList(Files.localInput(file1));

    // canMerge should return false due to null values
    boolean result = ParquetFileMerger.canMerge(inputFiles);
    assertThat(result)
        .as("canMerge should return false for files with null row lineage values")
        .isFalse();
  }

  @Test
  public void testMergeFilesVerifiesExactRowIdValues() throws IOException {
    // Test that verifies exact row ID and sequence number values in merged output
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    // Create file 1 with 3 records, firstRowId=100, dataSequenceNumber=5
    File file1 = new File(tempDir, "file1.parquet");
    createParquetFileWithData(
        Files.localOutput(file1),
        schema,
        Arrays.asList(
            createRecord(schema, 1, "a"),
            createRecord(schema, 2, "b"),
            createRecord(schema, 3, "c")));

    // Create file 2 with 2 records, firstRowId=200, dataSequenceNumber=7
    File file2 = new File(tempDir, "file2.parquet");
    createParquetFileWithData(
        Files.localOutput(file2),
        schema,
        Arrays.asList(createRecord(schema, 4, "d"), createRecord(schema, 5, "e")));

    // Create file 3 with 4 records, firstRowId=300, dataSequenceNumber=10
    File file3 = new File(tempDir, "file3.parquet");
    createParquetFileWithData(
        Files.localOutput(file3),
        schema,
        Arrays.asList(
            createRecord(schema, 6, "f"),
            createRecord(schema, 7, "g"),
            createRecord(schema, 8, "h"),
            createRecord(schema, 9, "i")));

    List<InputFile> inputFiles =
        Arrays.asList(Files.localInput(file1), Files.localInput(file2), Files.localInput(file3));

    // Merge files with row lineage preservation
    File outputFile = new File(tempDir, "merged.parquet");
    MessageType parquetSchema = ParquetFileMerger.readSchema(inputFiles.get(0));
    Map<String, String> metadata = ParquetFileMerger.readMetadata(inputFiles.get(0));

    ParquetFileMerger.mergeFiles(
        inputFiles,
        EncryptedFiles.plainAsEncryptedOutput(Files.localOutput(outputFile)),
        parquetSchema,
        Arrays.asList(100L, 200L, 300L), // firstRowIds for each file
        Arrays.asList(5L, 7L, 10L), // dataSequenceNumbers for each file
        TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT,
        ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
        metadata);

    // Read back the merged data and verify exact row ID values
    List<RowLineageRecord> records = readRowLineageData(Files.localInput(outputFile), schema);

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

  /** Helper to create a GenericRecord */
  private Record createRecord(Schema schema, int id, String data) {
    Record record = GenericRecord.create(schema);
    record.setField("id", id);
    record.setField("data", data);
    return record;
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
    var writer =
        org.apache.iceberg.parquet.Parquet.writeData(outputFile)
            .schema(schema)
            .withSpec(PartitionSpec.unpartitioned())
            .createWriterFunc(GenericParquetWriter::create)
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, String.valueOf(rowGroupSize))
            .overwrite()
            .build();
    try {
      for (Record record : records) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }
  }

  /** Helper to create a Parquet file with row lineage columns */
  private void createParquetFileWithRowLineage(
      OutputFile outputFile, Schema schema, List<Record> records) throws IOException {
    var writer =
        org.apache.iceberg.parquet.Parquet.writeData(outputFile)
            .schema(schema)
            .withSpec(PartitionSpec.unpartitioned())
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .build();
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
        org.apache.iceberg.parquet.Parquet.read(inputFile)
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
  private List<RowLineageRecord> readRowLineageData(InputFile inputFile, Schema baseSchema)
      throws IOException {
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

  /** Helper method to create a Parquet file with a given schema (empty file, just for testing) */
  private void createParquetFileWithSchema(OutputFile outputFile, Schema schema)
      throws IOException {
    org.apache.iceberg.parquet.Parquet.writeData(outputFile)
        .schema(schema)
        .withSpec(PartitionSpec.unpartitioned())
        .createWriterFunc(GenericParquetWriter::create)
        .overwrite()
        .build()
        .close();
  }
}
